using JobMaster.Abstractions;
using JobMaster.Abstractions.Models;
using JobMaster.Abstractions.Models.Attributes;
using JobMaster.Abstractions.StaticRecurringSchedules;
using TargetTestRecurringApp.Redis;

namespace TargetTestRecurringApp.Handlers;

// Registered as a static recurring schedule purely from this attribute -- no profile, no /recurring-schedule
// call needed. Proves RecurringScheduleAttribute-based auto-registration end-to-end (real container,
// real DB, real replanning), alongside the profile-based static schedules already covered by
// StaticTimeSpanIntervalProfile/StaticNaturalCronProfile.
[JobMasterDefinitionId("RecurringApp.AttributeTick")]
[TimeSpanIntervalSchedule("00:06:00")]
public sealed class AttributeStaticTickHandler(IExecutionRecorder recorder) : IJobMasterHandler
{
    public const string TestIdentifier = "attribute-static-tick";

    public Task HandleAsync(JobContext job) => recorder.RecordAsync(TestIdentifier, job.Id, "RecurringApp.AttributeTick");
}
