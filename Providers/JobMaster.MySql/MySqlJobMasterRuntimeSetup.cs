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
            throttlerSettingsTemplate: new OperationThrottlerSettingsTemplate(3, 2000));

        OperationThrottlerSettingsTemplateFactory.RegisterForAgent(
            RepositoryTypeId,
            maxBatchSize: 50,
            internalThrottlerSettingsTemplate: new OperationThrottlerSettingsTemplate(3, 2000),
            schedulingThrottlerSettingsTemplate: new OperationThrottlerSettingsTemplate(10, 750));

        await base.OnBeforeStartAsync(mainServiceProvider);
    }
}
