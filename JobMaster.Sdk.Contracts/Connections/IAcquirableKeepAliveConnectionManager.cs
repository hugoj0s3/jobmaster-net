using JobMaster.Sdk.Contracts.Config;

namespace JobMaster.Sdk.Contracts.Connections;

public interface IAcquirableKeepAliveConnectionManager<T>
{
    IAcquirableKeepAliveConnection<T> AcquireConnection(
        string connectionId,
        TimeSpan idleTimeTimeout,
        string connectionString, 
        JobMasterConfigDictionary? additionalConnConfig = null, 
        int maxGates = 1);
}