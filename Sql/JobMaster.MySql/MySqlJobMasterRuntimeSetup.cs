using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Repositories;
using JobMaster.SqlBase;

namespace JobMaster.MySql;

internal class MySqlJobMasterRuntimeSetup : SqlJobMasterRuntimeSetup
{
    public override string RepositoryTypeId => MySqlRepositoryConstants.RepositoryTypeId;

    public override async Task OnBeforeStartAsync(IServiceProvider mainServiceProvider)
    {
        OperationThrottlerSettingsTemplateFactory.RegisterForMaster(
            RepositoryTypeId,
            maxBatchSize: 50,
            throttlerSettingsTemplate: new OperationThrottlerSettingsTemplate(5));

        OperationThrottlerSettingsTemplateFactory.RegisterForAgent(
            RepositoryTypeId,
            maxBatchSize: 50,
            internalThrottlerSettingsTemplate: new OperationThrottlerSettingsTemplate(5),
            schedulingThrottlerSettingsTemplate: new OperationThrottlerSettingsTemplate(5, 500));

        await base.OnBeforeStartAsync(mainServiceProvider);
    }
}
