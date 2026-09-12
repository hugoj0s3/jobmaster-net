namespace JobMaster.Sdk.Utils.Extensions;

internal static class TypeExtensions
{
#if NETSTANDARD2_0
     internal static bool IsAssignableTo(this Type thisType, Type c) => c.IsAssignableFrom(thisType);
#endif
}