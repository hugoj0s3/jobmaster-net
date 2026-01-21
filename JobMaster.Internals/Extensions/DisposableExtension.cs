#if JOBMASTER
namespace JobMaster.Internals;
#elif NATJS
namespace JobMaster.NatJetStream.Internals.Utils;
#elif SQLPROV
namespace JobMaster.Sql.Internals.Utils;
#else
namespace JobMaster.Internal.Utils;
#endif

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