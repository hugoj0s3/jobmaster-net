using System.ComponentModel;
namespace JobMaster.Sdk.Abstractions.Ioc.Markups;

[EditorBrowsable(EditorBrowsableState.Never)]
public interface IJobMasterClusterAwareMasterRepository : IJobMasterClusterAwareComponent
{
    public string MasterRepoTypeId { get; }
}