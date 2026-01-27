namespace JobMaster.Sdk.Abstractions.Ioc.Markups;

internal interface IJobMasterClusterAwareMasterRepository : IJobMasterClusterAwareComponent
{
    public string MasterRepoTypeId { get; }
}