using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Repositories;
using JobMaster.SqlBase;

namespace JobMaster.SqlServer;

internal class SqlServerJobMasterRuntimeSetup : SqlJobMasterRuntimeSetup
{
    public override string RepositoryTypeId => SqlServerRepositoryConstants.RepositoryTypeId;

    public override async Task OnBeforeStartAsync(IServiceProvider mainServiceProvider)
    {
        OperationThrottlerSettingsTemplateFactory.RegisterForMaster(
            RepositoryTypeId,
            maxBatchSize: 50,
            throttlerSettingsTemplate: new OperationThrottlerSettingsTemplate(50));

        OperationThrottlerSettingsTemplateFactory.RegisterForAgent(
            RepositoryTypeId,
            maxBatchSize: 50,
            internalThrottlerSettingsTemplate: new OperationThrottlerSettingsTemplate(25),
            schedulingThrottlerSettingsTemplate: new OperationThrottlerSettingsTemplate(10, 500));

        await base.OnBeforeStartAsync(mainServiceProvider);
    }
}
