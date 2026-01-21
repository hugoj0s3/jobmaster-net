using System.ComponentModel;
using JobMaster.Sdk.Abstractions.Config;

namespace JobMaster.Sdk.Abstractions.Connections;

[EditorBrowsable(EditorBrowsableState.Never)]
public interface IAcquirableKeepAliveConnectionManager<T>
{
    IAcquirableKeepAliveConnection<T> AcquireConnection(
        string connectionId,
        TimeSpan idleTimeTimeout,
        string connectionString, 
        JobMasterConfigDictionary? additionalConnConfig = null, 
        int maxGates = 1);
}