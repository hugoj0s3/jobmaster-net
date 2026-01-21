namespace JobMaster.Sdk.Abstractions.Ioc.Markups;

public interface IJobMasterClusterAwareMasterRepository : IJobMasterClusterAwareComponent
{
    public string MasterRepoTypeId { get; }
}