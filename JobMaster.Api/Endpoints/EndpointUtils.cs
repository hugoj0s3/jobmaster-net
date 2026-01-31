using JobMaster.Api.AspNetCore.Auth;
using JobMaster.Api.AspNetCore.Internals;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Ioc.Markups;
using JobMaster.Sdk.Background;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace JobMaster.Api.Endpoints;

internal static class EndpointUtils
{
    public static RouteGroupBuilder GetJobMasterEndpointGroup(this IEndpointRouteBuilder endpoints)
    {
        var options = endpoints.GetJobMasterApiOptions();
        var basePath = JobMasterApiPath.NormalizeBasePath(options.BasePath);

        var group = endpoints.MapGroup(basePath);
        
        return group;
    }
 
    public static RouteGroupBuilder GetClusterEntityGroup(this RouteGroupBuilder group, string entityName)
    {
        var tag = string.IsNullOrEmpty(entityName)
            ? entityName
            : char.ToUpperInvariant(entityName[0]) + entityName.Substring(1);
        return group.MapGroup("/{clusterId}").MapGroup($"/{entityName}").WithTags(tag);
    }

    public static JobMasterApiOptions GetJobMasterApiOptions(this IEndpointRouteBuilder endpoints)
    {
        return endpoints.ServiceProvider.GetRequiredService<IOptions<JobMasterApiOptions>>().Value;
    }
    
    public static TComponent?
        GetClusterAwareComponent<TComponent>(string clusterId) 
        where TComponent : class, IJobMasterClusterAwareComponent
    {
        var config = JobMasterClusterConnectionConfig.TryGet(clusterId);
        if (config == null)
            return null;
        
        var factory = JobMasterClusterAwareComponentFactories.GetFactory(config.ClusterId);
        return factory?.GetComponent<TComponent>();
    } 
}