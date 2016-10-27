namespace Sitecore.Support.TimeCachedPropertyStore
{
  using System;
  using System.Collections.Concurrent;
  using System.Collections.Generic;
  using System.Data;
  using System.Data.SqlClient;
  using System.Linq;
  using System.Runtime.CompilerServices;
  using System.Threading;
  using Sitecore;
  using Sitecore.Abstract.Data.SqlServer;
  using Sitecore.Configuration;
  using Sitecore.Data;
  using Sitecore.Data.DataProviders;
  using Sitecore.Data.Eventing.Remote;
  using Sitecore.Data.Events;
  using Sitecore.Diagnostics;
  using Sitecore.Eventing;
  using Sitecore.StringExtensions;
  using Sitecore.Support.TimeCachedPropertyStore.DataModels;
  using Sitecore.Support.TimeCachedPropertyStore.RemoteEvents;

  public class DatabasePropertyProviderWithAsyncFlush : DatabasePropertiesProvider
  {           
    #region Constructor

    public DatabasePropertyProviderWithAsyncFlush(SqlServerDataProvider provider)
      : base(provider)
    {
      //In case Sitecore reflection did not set FlushInterval
      if (this.FlushInterval == TimeSpan.Zero)
        this.FlushInterval = Settings.GetTimeSpanSetting("Support.IntervalPropertyFlushProvider.DatabasePropertiesFlushChangesInterval", TimeSpan.FromSeconds(10));

      this.logDebugingInfo = Settings.GetBoolSetting("Support.IntervalPropertyFlushProvider.LogDebugInfo", false);

      //Specify less concurrency rate than by default.
      //By default, the dictionary creates 4 times the processor count of lock objects: http://arbel.net/2013/02/03/best-practices-for-using-concurrentdictionary/

      this.CachedValues = new ConcurrentDictionary<string, DatabasePropertyRowModel>(Environment.ProcessorCount, 150, StringComparer.OrdinalIgnoreCase);
      this.CachedPrefixes = new List<string>();

      //hack - provider database is null now.
      //so we cannot use Database = provider.Database
      ConnectionString = provider.Api.ConnectionString;


      //Subscribe on 2 default and one custom event.
      EventManager.Subscribe<PropertyRemovedRemoteEvent>(this.OnPropRemovedEvent);
      //Event would still be raised in case single property was modified.
      EventManager.Subscribe<PropertyChangedRemoteEvent>(this.OnPropertyChanged);

      //We raise one event for changed property instead of many small ones.
      EventManager.Subscribe<BulkPropertyChangeRemoteEvent>(this.OnBulkPropertyChange);

      //Try to flush values from memory in case system is about to shutdown.
      AppDomain.CurrentDomain.DomainUnload += this.FlushChangesOnDomainUnload;

      //Wait one flush interval until wake up
      //Cannot use heartbeat, since it can stuck on processing EventQueue. So changes will not be flushed.
      this.FlushTimer = new Timer(this.FlushChanges, null, this.FlushInterval, this.FlushInterval);
    }

    private void FlushChangesOnDomainUnload(object sender, EventArgs e)
    {
      FlushChanges(sender);
    }

    #endregion

    #region Instance fields

    private bool logDebugingInfo;

    private Database database;

    protected TimeSpan FlushInterval;

    /// <summary>
    /// Key - property name. Value can be <see cref=" UpdateDatabasePropertyRowModel"/>, or <see cref="DatabasePropertyRowModel"/>.
    /// <para>values of <see cref="UpdateDatabasePropertyRowModel"/> type are flushed periodically and transformed to <see cref="DatabasePropertyRowModel"/></para>
    /// </summary>
    protected ConcurrentDictionary<string, DatabasePropertyRowModel> CachedValues;

    /// <summary>
    /// Cached prefixes in upper case.
    /// </summary>
    protected List<string> CachedPrefixes;

    /// <summary>
    /// Cannot use heartbeat, since it can stuck ( f.e. process large batch of events )
    /// <para>Timer sends execution to managed thread pool workers.</para>
    /// </summary>
    protected Timer FlushTimer;

    /// <summary>
    /// Flag shows whether flush is going on. 
    /// <para>Avoid simultanious flush.</para>
    /// </summary>
    protected int Flushing;

    private readonly string ConnectionString;

    #endregion

    #region Properties

    public Database Database
    {
      get
      { //Dirty hack since cannot inject database via factory & reflection
        if (this.database == null)
        {
          this.database = (from database in Factory.GetDatabases()
                           where (!string.IsNullOrEmpty(database.ConnectionStringName))
                           let connection = Settings.GetConnectionString(database.ConnectionStringName)
                           where string.Equals(connection, ConnectionString, StringComparison.Ordinal)
                           select database).FirstOrDefault();
        }

        return this.database;
      }
    }
    #endregion

    #region Protected methods

    #region Flush changes
    /// <summary>
    /// Flushes the changes. Changes - values in <see cref="CachedValues"/> of type <see cref="UpdateDatabasePropertyRowModel"/>.
    /// <para>Uses <see cref="Flushing"/> value to avoid simultanious executions.</para>
    /// </summary>
    /// <param name="state">The state.</param>
    protected void FlushChanges(object state)
    {
      var flushing = Interlocked.CompareExchange(ref Flushing, 1, 0) == 1;
      if (flushing)
      {
        if (logDebugingInfo)
          this.LogDebugInfo("Previous flush iteration is still running.");
        return; // Still completing previous iteration. 
      }

      try
      {
        //Timer defaults to a thread pool thread, thus if current method raises an exception, application would restart.

        var candidates = this.GetUpdateCandidates();

        this.FlushCandidatesAndRaiseEvents(candidates);
      }
      catch (Exception ex)
      {
        //Exception in background thread 
        Log.Fatal("Could not queue FlushCandidatesAndRaiseEvents.", ex);
      }
      finally
      {
        Interlocked.CompareExchange(ref Flushing, 0, 1);
      }
    }

    /// <summary>
    /// Selects values from <see cref="CachedValues"/> with <see cref="UpdateDatabasePropertyRowModel"/> type.
    /// </summary>
    /// <returns></returns>
    [NotNull]
    protected virtual UpdateDatabasePropertyRowModel[] GetUpdateCandidates()
    {
      //Ienumarable is thread safe.
      return (from cachePair in CachedValues
              let casted = (cachePair.Value as UpdateDatabasePropertyRowModel)
              where casted != null
              select casted).ToArray();
    }

    /// <summary>
    /// Flushes the candidates to database.
    /// <para>Successfull submitions are removed from cache and remotes events are raised.</para>
    /// </summary>
    /// <param name="candidates">The candidates.</param>
    private void FlushCandidatesAndRaiseEvents([CanBeNull] UpdateDatabasePropertyRowModel[] candidates)
    {
      if ((candidates == null) || (candidates.Length == 0))
      {
        this.LogDebugInfo("No update candidates. Skipping..");
        return;
      }
      try
      {
        this.LogDebugInfo("Update Candidates found {0}. Going to flush changes".FormatWith(candidates.Length));

        var succesfullSubmitions = this.FlushChangesToDb(candidates);

        //Change cached UpdateDatabasePropertyRowModel value to DatabasePropertyRowModel, to avoid further flushes of this object.
        foreach (var propertyUpdateMetadata in succesfullSubmitions)
        {
          if (logDebugingInfo)
            this.LogDebugInfo("{0} was flushed. Value {1}".FormatWith(propertyUpdateMetadata.Key, propertyUpdateMetadata.Value));
          //represent as property info, so that value no longer treaded as change.
          TryAddOrUpdateInCache(propertyUpdateMetadata.AsPropertyInfo());
        }
        RaiseSubmittedEvents(succesfullSubmitions);

      }
      catch (Exception ex)
      {
        //Must not raise any exceptions at all in background thread !!
        Log.Error("Could not flush changes.", ex);
      }

    }

    private void RaiseSubmittedEvents([CanBeNull] UpdateDatabasePropertyRowModel[] succesfullSubmitions)
    {
      if ((succesfullSubmitions == null) || (succesfullSubmitions.Length == 0))
        return;

      var toBeNotified = (from submission in succesfullSubmitions
                          where !submission.UseEventDisabler
                          select submission).ToArray();

      if (toBeNotified.Length == 0)
        return;

      if (toBeNotified.Length == 1)
      {
        Database.RemoteEvents.Queue.QueueEvent(new PropertyChangedRemoteEvent(toBeNotified[0].Key));
      }
      else
      {
        Database.RemoteEvents.Queue.QueueEvent(new BulkPropertyChangeRemoteEvent(toBeNotified));
      }
    }

    /// <summary>
    /// Flushes the changes to database.
    /// </summary>
    /// <param name="changes">The changes.</param>
    /// <returns>Successfull submitions</returns>
    [NotNull]
    protected virtual UpdateDatabasePropertyRowModel[] FlushChangesToDb([CanBeNull] UpdateDatabasePropertyRowModel[] changes)
    {
      if ((changes == null) || (changes.Length == 0))
        return new UpdateDatabasePropertyRowModel[0];

      var flushed = new List<UpdateDatabasePropertyRowModel>(changes.Length);
      using (var connection = new SqlConnection(this.ConnectionString))
      {
        using (var command = new SqlCommand(@"EXEC UpsertDbProperty @key, @value", connection))
        {
          var key = command.Parameters.Add("@key", SqlDbType.NVarChar);
          var val = command.Parameters.Add("@value", SqlDbType.NText);
          //var addedDate = command.Parameters.Add("@AddedDate", SqlDbType.DateTime2);
          try
          {
            connection.Open();
            foreach (var updateCandidate in changes)
            {
              try
              {
                key.SqlValue = updateCandidate.Key;

                val.SqlValue = updateCandidate.Value;

                //addedDate.SqlValue = updateCandidate.AddedUTCTime;
                command.ExecuteNonQuery();

              }
              catch (Exception ex)
              {
                Log.Error("Flushing update candidates failed", ex, this);
                //TODO: Think about correct exception handling
                continue;
              }
              flushed.Add(updateCandidate);
            }
          }
          finally
          {
            connection.Close();
          }
        }
      }
      var res = flushed.ToArray();

      Assert.ResultNotNull(res, "an attempt to pass empty value");

      return res;
    }

    #endregion

    #region Event handlers

    protected virtual void OnPropRemovedEvent([NotNull] PropertyRemovedRemoteEvent @event, EventContext context)
    {
      Assert.ArgumentNotNull(@event, "event");
      try
      {
        this.RemovePropertyFromCache(@event.PropertyName, @event.IsPrefix);
      }
      catch (Exception ex)
      {
        Log.Error("Could not remove changed property {0}. IsPrefix {1}".FormatWith(@event.PropertyName, @event.IsPrefix), ex);
      }
    }

    protected void OnPropertyChanged(PropertyChangedRemoteEvent @event, EventContext context)
    {
      Assert.ArgumentNotNull(@event, "event");
      Assert.IsNotNullOrEmpty(@event.PropertyName, "event.PropertyName");
      OnPropertyChanged(@event.PropertyName);

    }

    protected virtual void OnPropertyChanged([NotNull]string propertyName)
    {
      Assert.ArgumentNotNullOrEmpty(propertyName, "propertyName");
      DatabasePropertyRowModel saved;
      CachedValues.TryRemove(propertyName, out saved);

      var prefix = CachedPrefixes.FirstOrDefault(propertyName.StartsWith);
      //We must reload value from database to keep integrity up-to-date.
      if (prefix != null)
      {
        lock (CachedPrefixes)
        {
          var reloadedValue = this.GetPropertyCore(propertyName);
          TryAddOrUpdateInCache(reloadedValue);
        }

      }
    }

    protected void OnBulkPropertyChange(BulkPropertyChangeRemoteEvent @event, EventContext context)
    {
      Assert.ArgumentNotNull(@event, "event");


      if ((@event.PropertyNames == null) || (@event.PropertyNames.Length == 0))
        return;

      foreach (var changedProp in @event.PropertyNames)
      {
        OnPropertyChanged(changedProp);
      }
    }

    #endregion

    #region Manipulations with Cache

    /// <summary>
    /// Updates value in cache in case cached value was added earlier than candidate (<see cref="DatabasePropertyRowModel.AddedUTCTime"/>).
    /// </summary>
    /// <param name="candidate"></param>
    /// <returns><value>false</value> when either candidate is null, or an error occurred during updating cache;otherwise <value>true</value></returns>
    protected virtual bool TryAddOrUpdateInCache([CanBeNull] DatabasePropertyRowModel candidate)
    {
      try
      {
        if (candidate == null)
          return false;

        CachedValues.AddOrUpdate(candidate.Key, candidate, (s, cached) =>
        {
          if (cached == null)
            return candidate;
          return (candidate.AddedUTCTime >= cached.AddedUTCTime) ? candidate : cached;
        }
          );
        return true;
      }
      catch (Exception ex)
      {
        //TODO: think about correct issue handling
        return false;
      }
    }

    /// <summary>
    /// Changes the value in cache for specified info. Does not take <see cref="DatabasePropertyRowModel.AddedUTCTime"/> time.
    /// </summary>
    /// <param name="info"></param>
    protected void UpdateInCache([NotNull] DatabasePropertyRowModel info)
    {
      Assert.ArgumentNotNull(info, "info");
      CachedValues[info.Key] = info;
    }

    /// <summary>
    /// Places the value into cache in case candidate is newer (<see cref="DatabasePropertyRowModel.AddedUTCTime" />) than current value.
    /// </summary>
    /// <param name="candidate">The candidate.</param>
    /// <param name="setValue">The set value.</param>
    /// <returns></returns>
    protected bool TryAddOrUpdateInCache([NotNull]DatabasePropertyRowModel candidate, out DatabasePropertyRowModel setValue)
    {
      Assert.ArgumentNotNull(candidate, "candidate");

      if (TryAddOrUpdateInCache(candidate))
        return CachedValues.TryGetValue(candidate.Key, out setValue);

      setValue = null;
      return false;
    }

    private void RemovePropertyFromCache([NotNull]string propertyNameToRemove, bool isPrefix)
    {
      Assert.ArgumentNotNullOrEmpty(propertyNameToRemove, "propertyNameToRemove");

      if (isPrefix)
      {
        lock (CachedPrefixes)
        {
          CachedPrefixes.RemoveAll(t => t.StartsWith(propertyNameToRemove));
        }
      }

      int removedFromCacheNumber = 0;

      // Enumeration through ConcurrentDictionary is thread safe both for reads and writes.
      // https://msdn.microsoft.com/en-us/library/dd287115(v=vs.110).aspx
      foreach (var keyPair in this.CachedValues)
      {
        var checkedKey = keyPair.Key;
        var shouldRemove = (isPrefix) ? checkedKey.StartsWith(propertyNameToRemove) : checkedKey.Equals(propertyNameToRemove, StringComparison.OrdinalIgnoreCase);
        if (shouldRemove)
        {
          DatabasePropertyRowModel popped;
          this.CachedValues.TryRemove(checkedKey, out popped);
          removedFromCacheNumber++;
          if (!isPrefix)
            return; //Culprit already found.
        }
      }
    }
    #endregion

    #region Logging methods

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    protected void LogDebugInfo(string message)
    {
      if (this.logDebugingInfo)
        Log.Debug("AsyncPropertyStore: " + message);
    }
    #endregion 
    #endregion

    #region DatabasePropertiesProvider Public methods override.

    #region GetProperties override
    /// <summary>
    /// Gets the property from cache first. If could not get from cache - makes a direct call to <see cref="GetPropertyCore"/> and caches the result.
    /// </summary>
    /// <param name="propertyName">Name of the property.</param>
    /// <param name="context">The context.</param>
    /// <param name="dataProvider">The data provider.</param>
    /// <returns></returns>
    [NotNull]
    [CaseInsensitive]
    public override string GetProperty([CanBeNull]string propertyName, CallContext context, SqlServerDataProvider dataProvider)
    {
      return string.IsNullOrEmpty(propertyName) ? string.Empty : this.GetProperty(propertyName);
    }

    /// <summary>
    /// Gets the property from cache firt. If could not get from cache - makes a direct call to <see cref="GetPropertyCore"/> and caches the result.
    /// <para>propertyName would be cached.</para>
    /// </summary>
    /// <param name="propertyName">Name of the property.</param>
    /// <returns>Model with empty string if value was not found in database; read value otherwise.</returns>
    [NotNull]
    protected DatabasePropertyRowModel GetPropertyRowModel([NotNull]string propertyName)
    {
      Assert.ArgumentNotNullOrEmpty(propertyName, "propertyName");

      DatabasePropertyRowModel res;

      if ((DatabaseCacheDisabler.CurrentValue == false) && //Respect cache disabler.
            this.CachedValues.TryGetValue(propertyName, out res)
        )
        return res;

      //Read value from database.
      res = this.GetPropertyCore(propertyName);

      DatabasePropertyRowModel upToDateValue;

      this.TryAddOrUpdateInCache(res, out upToDateValue);

      return upToDateValue;
    }
    /// <summary>
    /// Gets the property from cache first. If could not get from cache - makes a direct call to <see cref="GetPropertyCore"/> and caches the result.
    /// </summary>
    /// <param name="propertyName"></param>
    /// <returns></returns>
    [NotNull]
    protected string GetProperty([NotNull] string propertyName)
    {
      Assert.ArgumentNotNullOrEmpty(propertyName, "propertyName");
      return this.GetPropertyRowModel(propertyName);
    }

    /// <summary>
    /// Gets the property directly from database.
    /// </summary>
    /// <param name="propertyName">Name of the property.</param>
    /// <param name="context">The context.</param>
    /// <param name="dataProvider">The data provider.</param>
    /// <returns></returns>
    [NotNull]
    [CaseInsensitive]
    public virtual new DatabasePropertyRowModel GetPropertyCore(string propertyName, CallContext context, SqlServerDataProvider dataProvider)
    {
      Assert.IsNotNullOrEmpty(propertyName, "property name");
      return this.GetPropertyCore(propertyName);
    }

    /// <summary>
    /// Reads key from database directly.
    /// </summary>
    /// <param name="propertyName"></param>
    /// <returns>model with <value>string.Empty</value> value in case record does not exist in database.</returns>
    [NotNull]
    [CaseInsensitive]
    protected virtual DatabasePropertyRowModel GetPropertyCore([NotNull]string propertyName)
    {
      Assert.IsNotNullOrEmpty(propertyName, "property name");

      using (var connection = new SqlConnection(this.ConnectionString))
      {
        using (var command = new SqlCommand("SELECT TOP 1 [Key], [Value] FROM Properties WHERE [Key]=@key", connection))
        {
          command.Parameters.Add("@key", SqlDbType.NVarChar, propertyName.Length).SqlValue = propertyName;
          try
          {
            connection.Open();
            using (var rd = command.ExecuteReader(CommandBehavior.SingleRow))
            {
              var res = rd.Read() ? new DatabasePropertyRowModel(rd) : new EmptyCachedPropertyRowModel(propertyName);
              Assert.ResultNotNull(res, "method attempted to return null value.");
              return res;
            }
          }
          finally
          {
            connection.Close();
          }
        }
      }
    }

    #region Get Property Keys

    /// <summary>
    /// Gets the property keys via given prefix. Is cache based.
    /// </summary>
    /// <param name="prefix">The prefix.</param>
    /// <param name="context">The context.</param>
    /// <param name="dataProvider">The data provider.</param>
    /// <returns></returns>
    public override List<string> GetPropertyKeys([NotNull]string prefix, CallContext context, SqlServerDataProvider dataProvider)
    {
      Assert.ArgumentNotNullOrEmpty(prefix, "prefix");
      //Cache everything in upper register
      prefix = prefix.ToUpper();

      var resultingPrefixValues = new List<string>();

      //If we have already cached prefix, can just get values from cache.
      if (TryReadPropertyKeysFromCache(prefix, ref resultingPrefixValues))
        return resultingPrefixValues;

      //Could not read elements from cache. Going to database directly.
      foreach (var readFromDBProperty in this.GetPropertyKeysCore(prefix))
      {
        DatabasePropertyRowModel cachedValue;

        //Update candidates ( UpdateDatabasePropertyRowModel ) might have fresher information than database does.
        DatabasePropertyRowModel freshFromDb = readFromDBProperty;

        TryAddOrUpdateInCache(freshFromDb, out cachedValue);
        if (cachedValue != null)
        {
          resultingPrefixValues.Add(cachedValue.Value);
        }
      }

      lock (CachedPrefixes)
      {
        if (!CachedPrefixes.Contains(prefix)) //Could add in another thread.
          CachedPrefixes.Add(prefix);
      }
      //Return results from cache, cause new values could appear in cache during reading from database.
      return ReadPropertyKeysFromCache(prefix);
    }

    /// <summary>
    /// Reads the property keys from cache.
    /// </summary>
    /// <param name="prefix">The prefix.</param>
    /// <returns></returns>
    protected List<string> ReadPropertyKeysFromCache([NotNull]string prefix)
    {
      Assert.ArgumentNotNull(prefix, "prefix");
      //Everything is cached in upper format, so fetch key must be in upper as well.
#if DEBUG
      Assert.AreEqual(prefix, prefix.ToUpper(), "Prefix is not in upper format");
#endif

      return new List<string>(from cachedKeyValuePair in this.CachedValues
                              where cachedKeyValuePair.Key.StartsWith(prefix)

                              //We cache non-existing key with empty value to avoid useless requests.
                              //Since TicketManager cannot process empty values, we should remove those. . .
                              where !string.IsNullOrEmpty(cachedKeyValuePair.Value)

                              select cachedKeyValuePair.Key);
    }

    /// <summary>
    /// Tries to read property keys from cache.
    /// </summary>
    /// <param name="prefix">The prefix.</param>
    /// <param name="result">The result. Would be created if <value>null</value>; Matching elements would be added into it.</param>
    /// <returns><value>true</value> if prefix is already cached;false otherwise</returns>
    protected virtual bool TryReadPropertyKeysFromCache([NotNull]string prefix, [CanBeNull] ref List<string> result)
    {
      Assert.ArgumentNotNull(prefix, "prefix");

#if DEBUG
      Assert.AreEqual(prefix, prefix.ToUpper(), "Prefix is not in upper format");
#endif

      result = result ?? new List<string>();

      if (!this.CachedPrefixes.Any(t => t.StartsWith(prefix)))
        return false;

      result = this.ReadPropertyKeysFromCache(prefix);

      Assert.ResultNotNull(result, "an attempt to pass an empty object as result");

      return true;
    }
    /// <summary>
    /// Gets the property keys with given prefix directly from database.
    /// </summary>
    /// <param name="prefix">The prefix.</param>
    /// <returns></returns>
    protected List<DatabasePropertyRowModel> GetPropertyKeysCore(string prefix)
    {
      Assert.IsNotNullOrEmpty(prefix, "property name");

      using (var connection = new SqlConnection(this.ConnectionString))
      {
        using (var command = new SqlCommand("SELECT [Key], [Value] FROM Properties WHERE CHARINDEX(@key,[KEY])=1", connection))
        {
          List<DatabasePropertyRowModel> result = new List<DatabasePropertyRowModel>();
          command.Parameters.Add("@key", SqlDbType.NVarChar, prefix.Length).SqlValue = prefix;
          try
          {
            connection.Open();
            using (var rd = command.ExecuteReader())
            {
              while (rd.Read())
              {
                var key = rd[0] as string;
                if (key == null)
                  continue;

                var val = rd[1] as string;

                result.Add(new DatabasePropertyRowModel(key, val));
              }
            }
            return result;
          }
          finally
          {
            connection.Close();
          }
        }
      }
    }

    #endregion

    #endregion

    #region RemoveProperty override

    [CaseInsensitive]
    public override bool RemoveProperty([CanBeNull]string propertyNameToRemove, bool isPrefix, CallContext context, SqlServerDataProvider dataProvider)
    {
      if (string.IsNullOrEmpty(propertyNameToRemove))
        return false;

      propertyNameToRemove = propertyNameToRemove.ToUpper();

      if (!this.RemovePropertyCore(propertyNameToRemove, isPrefix, context, dataProvider))
        return false;
      this.RemovePropertyFromCache(propertyNameToRemove, isPrefix);

      if (!EventDisabler.IsActive)
      {
        //TODO: Do we want to provoke incorrect values being read?
        this.Database.RemoteEvents.Queue.QueueEvent(new PropertyRemovedRemoteEvent(propertyNameToRemove, isPrefix));
      }

      return true;
    }



    public override bool RemovePropertyCore(string propertyNameToRemove, bool isPrefix, CallContext context, SqlServerDataProvider dataProvider)
    {
      try
      {
        //starts with
        string deleteCondition = isPrefix ? "charindex(@key,[Key])=1" : "[Key]=@key";
        using (var connection = new SqlConnection(this.ConnectionString))
        {
          using (var command = new SqlCommand("DELETE FROM Properties WHERE " + deleteCondition, connection))
          {
            command.Parameters.Add("@key", SqlDbType.NVarChar, propertyNameToRemove.Length).SqlValue = propertyNameToRemove;
            try
            {
              connection.Open();
              int debuggingResultCount = command.ExecuteNonQuery();
              return true;
            }
            finally
            {
              connection.Close();
            }
          }
        }
      }
      catch (Exception)
      {
        return false;
      }


    }

    #endregion

    #region SetProperty override
    public override bool SetProperty(string parameterName, string value, CallContext context, SqlServerDataProvider dataProvider)
    {
      if (string.IsNullOrEmpty(parameterName))
        return false;

      var updateCandidate = new UpdateDatabasePropertyRowModel(parameterName, value, EventDisabler.IsActive);

      if (ImmidiatePropertyFlushContext.IsActive)
      {
        this.FlushCandidatesAndRaiseEvents(new[]
        {
          updateCandidate
        });
      }
      //Make sure we load value from database before making decision on raising upsert query.
      //however cached 
      DatabasePropertyRowModel cached = this.GetPropertyRowModel(parameterName);

      if (ShouldSkipUpdate(updateCandidate, cached))
        return false;

      this.CachedValues.AddOrUpdate(updateCandidate.Key, updateCandidate,
        (s, currentCachedValue) =>
        {
          //no property in database with this key or key did not exist, an empty empty record was cached.
          if ((currentCachedValue == null) || (string.IsNullOrEmpty(currentCachedValue.Value) && (currentCachedValue is EmptyCachedPropertyRowModel)))
            return updateCandidate;

          if (string.Equals(currentCachedValue.Value, updateCandidate.Value, StringComparison.Ordinal))
          {
            //Value has not changed. Need to update AddedUTC in memory only (without going to database) to solve further collisions.
            if (updateCandidate.AddedUTCTime > currentCachedValue.AddedUTCTime)
              currentCachedValue.AddedUTCTime = updateCandidate.AddedUTCTime;
            return currentCachedValue;
          }

          //Last updated wins.
          return (currentCachedValue.AddedUTCTime > updateCandidate.AddedUTCTime) ? currentCachedValue : updateCandidate;
        });

      return true;
    }

    /// <summary>
    /// Shoulds the skip update.
    /// </summary>
    /// <param name="updateCandidate">The update candidate.</param>
    /// <param name="cached">The cached.</param>
    /// <returns></returns>
    protected virtual bool ShouldSkipUpdate(UpdateDatabasePropertyRowModel updateCandidate, DatabasePropertyRowModel cached)
    {
      return false;
    }

    #endregion

    #endregion

  }
}
