using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Models.Logs;

namespace JobMaster.Sdk.Background.Runners.JobAndRecurringScheduleLifeCycleControl;

internal class FallbackBucketPulseRunner : JobMasterRunner
{
    private readonly IJobsExecutionEngine engine;

    public override TimeSpan SucceedInterval => TimeSpan.FromMilliseconds(250);

    public FallbackBucketPulseRunner(IJobMasterBackgroundAgentWorker backgroundAgentWorker, IJobsExecutionEngine engine)
        : base(backgroundAgentWorker, bucketAwareLifeCycle: true, useSemaphore: false)
    {
        this.engine = engine;
    }

    public override async Task<OnTickResult> OnTickAsync(CancellationToken ct)
    {
        await engine.PulseAsync();
        return OnTickResult.Success(this);
    }
}
