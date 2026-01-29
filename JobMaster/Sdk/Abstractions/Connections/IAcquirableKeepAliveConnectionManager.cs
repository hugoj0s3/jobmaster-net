using JobMaster.Sdk.Abstractions.Config;

namespace JobMaster.Sdk.Abstractions.Connections;

internal interface IAcquirableKeepAliveConnectionManager<T>
{
    IAcquirableKeepAliveConnection<T> AcquireConnection(
        string connectionId,
        TimeSpan idleTimeTimeout,
        string connectionString, 
        JobMasterConfigDictionary? additionalConnConfig = null, 
        int maxGates = 1);
}