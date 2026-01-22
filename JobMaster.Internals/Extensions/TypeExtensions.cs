#if JOBMASTER
namespace JobMaster.Internals;
#elif NATJS
namespace JobMaster.NatJetStream.Internals.Utils;
#elif SQLPROV
namespace JobMaster.SqlBase.Internals.Utils;
#else
namespace JobMaster.Internal.Utils;
#endif

internal static class TypeExtensions
{
#if NETSTANDARD
     internal static bool IsAssignableTo(this Type thisType, Type c) => c.IsAssignableFrom(thisType);
#endif
}