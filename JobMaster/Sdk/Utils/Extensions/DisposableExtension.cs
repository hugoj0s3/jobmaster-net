namespace JobMaster.Sdk.Utils.Extensions;

internal static class DisposableExtension
{
    public static void SafeDispose(this IDisposable disposable)
    {
        try
        {
            disposable.Dispose();
        }
        catch
        {
            // Swallow
        }
    }
}