namespace JobMaster.Sdk.Utils.Extensions;

internal static class TypeExtensions
{
#if NETSTANDARD
     internal static bool IsAssignableTo(this Type thisType, Type c) => c.IsAssignableFrom(thisType);
#endif
}