namespace Sitecore.Support.TimeCachedPropertyStore.DataModels
{
  using System;

  public class EmptyCachedPropertyRowModel:DatabasePropertyRowModel
  {
    public EmptyCachedPropertyRowModel([NotNull] string key, DateTime? added = null) : base(key, null, added)
    {
    }
  }
}
