namespace Sitecore.Support.TimeCachedPropertyStore.DataModels
{
  using System;
  using System.Data.SqlClient;
  using System.Diagnostics;
  using Sitecore.Diagnostics;
  using Sitecore.StringExtensions;

  [CaseInsensitive]
  [DebuggerDisplay("{Key} VALUE : {Value} ADDED: {AddedUTCTime}")]
  public class DatabasePropertyRowModel
  {
    #region Instance fields

    public readonly string Key;

    public string Value;

    /// <summary>
    /// Should consider expanding properties table with this column to track modification versions.
    /// </summary>
    public DateTime AddedUTCTime;

    private readonly int hash;

    #endregion

    #region constructors
    /// <summary>
    /// Initializes a new instance of the <see cref="DatabasePropertyRowModel"/> class.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="value">The value.</param>
    /// <param name="added">The added.</param>
    public DatabasePropertyRowModel([NotNull] string key, [CanBeNull] string value = null, DateTime? added = null)
    {
      Assert.ArgumentNotNullOrEmpty(key, "key");

      this.Key = key.ToUpper();
      if (key.Length > 256)
        throw new ArgumentOutOfRangeException("key", "Database property Key column can have up to 256 characters. An attempt to put {0} is prevented".FormatWith(Key.Length));

      this.hash = Key.GetHashCode();

      this.Value = value ?? string.Empty;

      this.AddedUTCTime = added ?? DateTime.UtcNow;
    }


    /// <summary>
    /// Initializes a new instance of the <see cref="DatabasePropertyRowModel"/> class from reader. It must have Key, Value columns.
    /// </summary>
    /// <param name="reader">The reader.</param>
    /// <exception cref="System.ArgumentException">Readed does not have one of required columns</exception>
    public DatabasePropertyRowModel([NotNull]SqlDataReader reader)
    {
      Assert.IsNotNull(reader, "reader");
      Assert.IsTrue(reader.HasRows, "no data in reader");
      var key = reader["Key"] as string;
      if (key == null)
        throw new ArgumentException("Readed does not have 'Key' column");

      var val = reader["Value"] as string;

      //DateAdded is not null via DB constraints.
      //var dt = (DateTime)reader["DateAdded"];

      this.Key = key;
      this.Value = val ?? string.Empty;

      this.AddedUTCTime = DateTime.UtcNow;
    }

    #endregion


    public override int GetHashCode()
    {
      //Should override that one as we work with concurrent dictionary, which gets locks according to object`s hashcode.
      return this.hash;
    }

    [NotNull]
    public EmptyCachedPropertyRowModel AsEmpty()
    {
      return new EmptyCachedPropertyRowModel(this.Key, this.AddedUTCTime);
    }

    public static implicit operator string(DatabasePropertyRowModel obj)
    {
      return obj == null ? null : obj.Value;
    }
  }
}
