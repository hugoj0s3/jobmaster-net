namespace JobMaster.Abstractions.Models;

// UseRetryDelays is 0 so default(JobRetryDecision) means "use the cluster's RetryDelays".
internal enum JobRetryDecisionKind
{
    UseRetryDelays = 0,
    RetryAfter,
    StopEarly,
}
