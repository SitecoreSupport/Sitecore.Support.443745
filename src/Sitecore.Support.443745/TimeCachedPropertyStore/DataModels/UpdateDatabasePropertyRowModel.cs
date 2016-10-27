namespace Sitecore.Support.TimeCachedPropertyStore.DataModels
{
  using System;
  using System.Diagnostics;

  [CaseInsensitive]
  [DebuggerDisplay("{Key} VALUE : {Value} ADDED: {AddedUTCTime}")]
  public class UpdateDatabasePropertyRowModel : DatabasePropertyRowModel, IComparable<UpdateDatabasePropertyRowModel>
  {

    /// <summary>
    /// Should or not raise remote event about property change.
    /// </summary>
    public readonly bool UseEventDisabler;
    
    public UpdateDatabasePropertyRowModel([NotNull]string key, string newValue, bool useDisabler)
      : base(key, newValue)
    {
      this.UseEventDisabler = useDisabler;
    }

    public int CompareTo([CanBeNull]UpdateDatabasePropertyRowModel other)
    {
      if (other == null)
        return 1;
      return this.Key.Equals(other.Key, StringComparison.Ordinal) ? this.AddedUTCTime.CompareTo(other.AddedUTCTime) : String.Compare(this.Key, other.Key, StringComparison.Ordinal);
    }

    public static implicit operator string(UpdateDatabasePropertyRowModel obj)
    {
      return obj == null ? null : obj.Value;
    }

    [NotNull]
    public DatabasePropertyRowModel AsPropertyInfo()
    {
      return new DatabasePropertyRowModel(this.Key, this.Value, this.AddedUTCTime);
    }

  }
}