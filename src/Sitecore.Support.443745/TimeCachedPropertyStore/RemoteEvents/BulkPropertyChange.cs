namespace Sitecore.Support.TimeCachedPropertyStore.RemoteEvents
{
  using System.Collections.Generic;
  using System.Linq;
  using System.Runtime.Serialization;
  using Sitecore.Diagnostics;
  using Sitecore.Support.TimeCachedPropertyStore.DataModels;

  [DataContract]
  public class BulkPropertyChangeRemoteEvent
  {

    public BulkPropertyChangeRemoteEvent([NotNull]IEnumerable<string> changedKeys)
    {
      Assert.ArgumentNotNull(changedKeys, nameof(changedKeys));
      this.PropertyNames = changedKeys.ToArray();
    }

    public BulkPropertyChangeRemoteEvent([NotNull]IEnumerable<DatabasePropertyRowModel> changedKeys)
    {
      Assert.ArgumentNotNull(changedKeys, nameof(changedKeys));

      this.PropertyNames = changedKeys.Select(x => x.Key).ToArray();
    }

    /// <summary>
    /// Gets or sets the names of changed properties
    /// </summary>
    /// <value>The name of the property.</value>
    [DataMember]
    public string[] PropertyNames
    {
      get;
      set;
    }
  }
}
