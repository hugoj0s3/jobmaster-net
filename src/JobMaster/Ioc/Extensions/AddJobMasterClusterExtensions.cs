using JobMaster.Contracts.Ioc.Selectors;
using JobMaster.Contracts.Serialization;
using JobMaster.Sdk.Contracts;
using JobMaster.Sdk.Contracts.Ioc.Definitions;
using JobMaster.Sdk.Contracts.Ioc.Selectors;
using Microsoft.Extensions.DependencyInjection;

namespace JobMaster.Ioc.Extensions;

public static class AddJobMasterClusterExtensions
{ 
    public static void DefineJobMasterMessageJsonSerializer(this IServiceCollection services, IJobMasterMessageJsonSerializer serializer)
    {
        BootstrapBlueprintDefinitions.JobMasterJsonSerializer = serializer;
        JobMasterMessageJson.SetMessageSerializer(serializer);
    }
    
    public static IServiceCollection AddJobMasterCluster(this IServiceCollection services, string clusterId, Action<IClusterConfigSelector> configure)
    {
        if (services == null) throw new ArgumentNullException(nameof(services));
        if (configure == null) throw new ArgumentNullException(nameof(configure));

        var builder = ClusterConfigSelectorAdvancedFactory.Create(clusterId, services);
        configure(builder);
        
        if (BootstrapBlueprintDefinitions.JobMasterJsonSerializer == null)
        { 
            BootstrapBlueprintDefinitions.JobMasterJsonSerializer = new DefaultJobMasterMessageJsonSerializer();
        }
        JobMasterMessageJson.SetMessageSerializerIfNotSet(BootstrapBlueprintDefinitions.JobMasterJsonSerializer);
        
        if (BootstrapBlueprintDefinitions.JobMasterScheduler == null)
        {
            BootstrapBlueprintDefinitions.JobMasterScheduler = JobMasterScheduler.Instance;
        }
        
        if (BootstrapBlueprintDefinitions.JobMasterRuntime == null)
        {
            BootstrapBlueprintDefinitions.JobMasterRuntime = JobMasterRuntimeSingleton.Instance;
        }
        
        builder.Finish();
        
        return services;
    }

    public static IServiceCollection AddJobMasterCluster(this IServiceCollection services, Action<IClusterConfigSelector> configure)
    {
        if (services == null) throw new ArgumentNullException(nameof(services));
        if (configure == null) throw new ArgumentNullException(nameof(configure));
        
        var builder = ClusterConfigSelectorAdvancedFactory.Create(null, services);
         configure(builder);
        
        if (BootstrapBlueprintDefinitions.JobMasterJsonSerializer == null)
        { 
            BootstrapBlueprintDefinitions.JobMasterJsonSerializer = new DefaultJobMasterMessageJsonSerializer();
        }
        JobMasterMessageJson.SetMessageSerializerIfNotSet(BootstrapBlueprintDefinitions.JobMasterJsonSerializer);

        if (BootstrapBlueprintDefinitions.JobMasterRuntime == null)
        {
            BootstrapBlueprintDefinitions.JobMasterRuntime = JobMasterRuntimeSingleton.Instance;
        }
        
        if (BootstrapBlueprintDefinitions.JobMasterScheduler == null)
        {
            BootstrapBlueprintDefinitions.JobMasterScheduler = JobMasterScheduler.Instance;
        }
        
        builder.Finish();
        
        return services;
    }
}
