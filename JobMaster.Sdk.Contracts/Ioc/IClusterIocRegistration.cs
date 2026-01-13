using JobMaster.Sdk.Contracts.Ioc.Markups;
using JobMaster.Sdk.Contracts.Repositories.Agent;
using Microsoft.Extensions.DependencyInjection;

namespace JobMaster.Sdk.Contracts.Ioc;

public interface IClusterIocRegistration
{
    string ClusterId { get; }
    IJobMasterClusterAwareComponentFactory BuildFactory();
    
    IServiceCollection ClusterServices { get; }
    
    void AddJobMasterComponent<TService, TImplementation>() 
        where TService : class, IJobMasterClusterAwareComponent
        where TImplementation : class, TService;
    
    void AddJobMasterComponent<TService>(TService instance) 
        where TService : class, IJobMasterClusterAwareComponent;
    
    void AddJobMasterComponent<TService>(Func<IServiceProvider, TService> factory) 
        where TService : class, IJobMasterClusterAwareComponent;
    
    void AddRepositoryDispatcher<TRepositoryDispatcher, TSavePendingRepository, TProcessingRepository>(string repositoryTypeId) 
        where TRepositoryDispatcher : class, IAgentJobsDispatcherRepository<TSavePendingRepository, TProcessingRepository>
        where TSavePendingRepository : class, IAgentRawMessagesDispatcherRepository
        where TProcessingRepository : class, IAgentRawMessagesDispatcherRepository;
    
    void AddRepositoryDispatcher<TRepositoryDispatcher>(string repositoryTypeId) 
        where TRepositoryDispatcher : class, IAgentJobsDispatcherRepository;
}