# Plan 0.0.12 Alpha
## Changes planed for 0.0.12
### Make the cluster id configurable via attribute like the lane and etc..
    The idea is the same as the others, if the cluster id is not explicity specify fallback to the cluster id via attribute then fallback to the default cluster id.
    Add also on JobDefinitionConfig as optional.
    the recurring schedule attributes will respect the cluster id as the other configuration properties.
    Ensure all is covered by unit tests.
### Change the contract of the JobMasterScheduler. The idea is shown the job are accepted and stored on transport layer.
 - JobScheduleResult is now a Task<JobScheduleResult> instead of JobContext directly. the result will be the same for sync and async method.
    public class JobScheduleResult
    {
        public JobScheduleResultStatus Status { get; } // AcceptedOnTransport, StoredOnMaster
        public string? BucketId { get; }
        public string? WorkerId { get; }
        public JobContext Context { get; }
        public Task<bool> ConfirmOnMasterAsync(); // force the master to confirm the job. documented that it is not recommended specially for performance reason.
    }
 - AcceptedOnTransport means the job was accepted by the transport layer, it is not stored on the master (PendingSave)
 - StoredOnMaster means the job was stored on the master
 - Do the same for recurring schedule.
### Update naturalcron reference to newest version on nuget. there was fix recently.
 - check also if we need to plan code or something on the compiler.
 - PR: https://github.com/hugoj0s3/NaturalCron/pull/8
 
### Flush into master as safe net after 5 min. based on creation time.
bool ShouldFlush()
{
    return AcceptedJobs.Any(x => x.ConfirmationRequested)
        || AcceptedJobs.Count(x => x.CreatedAt < DateTime.UtcNow.AddMinutes(-5)) >= 100
        || AcceptedJobs.Any(x => x.CreatedAt < DateTime.UtcNow.AddMinutes(-10));
}

const int FlushBatchSize = 500;

void FlushAcceptedJobs()
{
    if (!ShouldFlush())
        return;

    var batch = AcceptedJobs
        .OrderBy(x => x.ConfirmationRequested ? 0 : 1) // private dto.
        .ThenBy(x => x.CreatedAt)
        .Take(FlushBatchSize)
        .ToList();

    InsertIfNotExists(batch);

    foreach (var job in batch)
        AcceptedJobs.Remove(job);
}
- The flush should be done in timer
- Something like the code above, flush can also be addaptive based on the number of unconfirmed jobs.
    - normal flow check flush every 5 sec based on ShouldFlush, but if there too many unconfirmed jobs flush every 50ms.
- The same for recurring schedule. similar dto and etc...
- Add this on JobMasterSchedulerClusterAware.
- The ConfirmationOnMasterAsync basically set the confirmation requested to true and force the flush.

## From Reminders.md
### JobMasterScheduler was public by oversight, not design — fixed 2026-09-19, watch for other bootstrap-plumbing classes with the same slip
### JobMasterDefinitionIdAttribute.GetJobHandlerTypeFromId re-scans all assemblies per distinct handler type
### Hybrid worker concept: independent transport configuration per connection role
    It will be design a bit different from the reminder file. We will have a hybrid connection instead. We need to define how the finger-print of the hybrid connection will be calculated.
    AddAgentConnection("hybrid-connection-1")
        .Hybrid()
        .Transport([nats-connection])
        .Execution([raven-db]);
    the workers will be attached to the hybrid connection. The repotype might return as nats+ravendb, or hybrid:nats+ravendb, the same approach for finger-print.
    we will need to figure out how the ioc will work with hybrid connections. also the nats validation will be different, 
    e.g only requires 5 min TransientThreshold if we have nats for the execution.
### CronosExprExtensions/NCrontabExprExtensions have colliding method names
   - Consider add method for NaturalCron that receives a string as well.
### Review Reminders.md and remove the ones already done.
### Update the documentation on C:\Users\hugo8\RiderProjects\jobmaster-doc
### After item right above. consider do better structure for the documentation.
- Focusing on the standalone mode, cover the basics firsts.
- Create presenting.md file for reference, it will be article later on.