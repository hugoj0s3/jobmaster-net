using System.ComponentModel;
using JobMaster.Sdk.Abstractions.Config;

namespace JobMaster.Sdk.Abstractions.Ioc.Markups;

[EditorBrowsable(EditorBrowsableState.Never)]
public interface IJobMasterClusterAwareComponent
{ 
    JobMasterClusterConnectionConfig ClusterConnConfig { get; }
}