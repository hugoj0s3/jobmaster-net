using JobMaster.Contracts;
using JobMaster.Contracts.Models.Attributes;
using JobMaster.Sdk.Contracts.Ioc;
using JobMaster.Sdk.Contracts.Ioc.Definitions;

namespace JobMaster.Sdk.Background;

public class DefaultRuntimeValidatorSetup : IJobMasterRuntimeSetup
{
    public Task<IList<string>> ValidateAsync(IServiceProvider mainServiceProvider)
    {
        var clustersWithTransientThresholdGreaterThan24Hours = BootstrapBlueprintDefinitions.Clusters.Where(x => x.TransientThreshold > TimeSpan.FromHours(24));
        if (clustersWithTransientThresholdGreaterThan24Hours.Any())
        {
            return Task.FromResult<IList<string>>(new List<string>());
        }
        
        var result = new List<string>();
        foreach (var cluster in clustersWithTransientThresholdGreaterThan24Hours)
        {
            result.Add($"{cluster.ClusterId} is configured with a TransientThreshold greater than 24 hours. This is not allowed.");
        }
        
        var assemblies = AppDomain.CurrentDomain.GetAssemblies()
            .Where(a => !a.IsDynamic && !string.IsNullOrEmpty(a.Location))
            .ToList();
        
        var handlerTypes = assemblies
            .SelectMany(a => a.GetTypes())
            .Where(t => typeof(IJobHandler).IsAssignableFrom(t) && 
                        !t.IsInterface && 
                        !t.IsAbstract)
            .Select(x => new
            {
                Type = x,
                JobDefinitionId = JobMasterDefinitionIdAttribute.GetJobDefinitionId(x)
            })
            .ToList();
        
       var handlerTypesWithDuplicateJobDefinitionIds = handlerTypes.GroupBy(x => x.JobDefinitionId)
            .Where(g => g.Count() > 1)
            .Select(g => g.Key)
            .ToList();
        
       if (handlerTypesWithDuplicateJobDefinitionIds.Any())
       {
           result.Add($"Multiple job handlers found with the same JobDefinitionId: {string.Join(", ", handlerTypesWithDuplicateJobDefinitionIds)}");
       }

        var workersWithoutAgentConnectionName = BootstrapBlueprintDefinitions.Clusters.SelectMany(x => x.Workers).Where(x => string.IsNullOrEmpty(x.AgentConnectionName)).ToList();
        if (workersWithoutAgentConnectionName.Any())
        {
            result.Add($"Workers without AgentConnectionName: {string.Join(", ", workersWithoutAgentConnectionName.Select(x => x.WorkerName))}");
        }
        
        return Task.FromResult<IList<string>>(result);
    }

    public Task OnStartingAsync(IServiceProvider mainServiceProvider)
    {
       return Task.CompletedTask;
    }
}