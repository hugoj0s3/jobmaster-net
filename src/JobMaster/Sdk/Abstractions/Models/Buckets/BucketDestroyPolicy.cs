using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Services.Agent;

namespace JobMaster.Sdk.Abstractions.Models.Buckets;

internal static class BucketDestroyPolicy
{
    /// <summary>
    /// Whether <paramref name="bucket"/> is safe to destroy right now. Fallback buckets are always
    /// safe — they never carry save-pending jobs, only jobs reserved for execution, which recover
    /// independently through the deadline runner (see AssignedLostBucketsRunner). Every other bucket
    /// type — including standalone buckets, which are ordinary buckets that merely use the reserved
    /// standalone connection — is safe only once it has no jobs left, so this deliberately keys off
    /// BucketType.Fallback and not "does this bucket use a reserved connection."
    /// </summary>
    public static async Task<bool> CanBeSafelyDestroyedAsync(IAgentJobsDispatcherService dispatcher, BucketModel bucket)
    {
        if (bucket.BucketType == BucketType.Fallback)
        {
            return true;
        }

        return !await dispatcher.HasJobsAsync(bucket.AgentConnectionId, bucket.Id);
    }
}