namespace JobMaster.Sdk.Contracts.Ioc.Markups;

public interface IJobMasterClusterAwareMasterRepository : IJobMasterClusterAwareComponent
{
    public string MasterRepoTypeId { get; }
}