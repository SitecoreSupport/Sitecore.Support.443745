namespace Sitecore.Support.TimeCachedPropertyStore
{
  using System;

  /// <summary>
  /// Class or Method does not take keys case (upper or lower) into account.
  /// </summary>
  [AttributeUsage(AttributeTargets.Method|AttributeTargets.Class)]
  public class CaseInsensitiveAttribute:Attribute
  {
  }
}
