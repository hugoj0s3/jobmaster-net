namespace JobMaster.Contracts.Extensions;

internal static class ObjectExtensions
{
    internal static T NotNull<T>(this T? obj)
    {
        return obj ?? throw new ArgumentNullException(nameof(obj));
    }
}