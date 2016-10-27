namespace Sitecore.Support.TimeCachedPropertyStore
{
  using System;
  using System.Threading;

  /// <summary>
  /// Wrap call to <see cref="DatabasePropertyProviderWithAsyncFlush.SetProperty"/> into this switcher to get values updated in db immidiatelly.
  /// </summary>
  public class ImmidiatePropertyFlushContext : IDisposable
  {

    public ImmidiatePropertyFlushContext()
    {
      Interlocked.Increment(ref _callNumber);
    }

    [ThreadStatic]
    private static int _callNumber;

    public static bool IsActive
    {
      get
      {
        return _callNumber > 0;
      }
    }

    public void Dispose()
    {
      Interlocked.Decrement(ref _callNumber);
    }
  }
}
