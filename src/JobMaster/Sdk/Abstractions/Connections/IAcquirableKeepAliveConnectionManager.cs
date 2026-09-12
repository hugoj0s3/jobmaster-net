using JobMaster.Sdk.Abstractions.Config;

namespace JobMaster.Sdk.Abstractions.Connections;

internal interface IAcquirableKeepAliveConnectionManager<T> : IDisposable
{
    IAcquirableKeepAliveConnection<T> AcquireConnection(
        string connectionId,
        TimeSpan idleTimeTimeout,
        string connectionString, 
        JobMasterConfigDictionary? additionalConnConfig = null, 
        int maxGates = 1);
}