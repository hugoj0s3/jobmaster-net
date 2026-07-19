namespace JobMaster.Sdk.Abstractions.Ioc;

internal interface IJobMasterRuntimeSetup
{
    Task<IList<string>> ValidateAsync(IServiceProvider mainServiceProvider);
    
    Task OnBeforeStartAsync(IServiceProvider mainServiceProvider);
}