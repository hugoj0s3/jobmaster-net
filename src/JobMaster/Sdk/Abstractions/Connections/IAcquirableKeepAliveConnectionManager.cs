using JobMaster.Sdk.Abstractions.Config;

namespace JobMaster.Sdk.Abstractions.Connections;

public interface IAcquirableKeepAliveConnectionManager<T>
{
    IAcquirableKeepAliveConnection<T> AcquireConnection(
        string connectionId,
        TimeSpan idleTimeTimeout,
        string connectionString, 
        JobMasterConfigDictionary? additionalConnConfig = null, 
        int maxGates = 1);
}