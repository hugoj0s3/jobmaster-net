using System.ComponentModel;
namespace JobMaster.Sdk.Abstractions.Ioc;

[EditorBrowsable(EditorBrowsableState.Never)]
public interface IJobMasterRuntimeSetup
{
    Task<IList<string>> ValidateAsync(IServiceProvider mainServiceProvider);
    
    Task OnStartingAsync(IServiceProvider mainServiceProvider);
}