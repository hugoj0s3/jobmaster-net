namespace JobMaster.Sdk.Abstractions.Ioc;

public interface IJobMasterRuntimeSetup
{
    Task<IList<string>> ValidateAsync(IServiceProvider mainServiceProvider);
    
    Task OnStartingAsync(IServiceProvider mainServiceProvider);
}