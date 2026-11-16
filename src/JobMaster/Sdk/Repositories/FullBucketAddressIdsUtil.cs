using JobMaster.Sdk.Abstractions;

namespace JobMaster.Sdk.Repositories;

internal static class FullBucketAddressIdsUtil
{
    public static string GetJobSavePendingBucketAddress(string? bucketId)
    {
        if (string.IsNullOrEmpty(bucketId))
        {
            throw new ArgumentNullException(nameof(bucketId));
        }

        return $"{bucketId}:Job-SavePending";
    }

    public static string GetJobProcessingBucketAddress(string? bucketId)
    {
        if (string.IsNullOrEmpty(bucketId))
        {
            throw new ArgumentNullException(nameof(bucketId));
        }

        return $"{bucketId}:Job-Processing";
    }

    public static string GetRecurringScheduleSavePendingBucketAddress(string? bucketId)
    {
        if (string.IsNullOrEmpty(bucketId))
        {
            throw new ArgumentNullException(nameof(bucketId));
        }

        return $"{bucketId}:RecurSchedule-SavePending";
    }

    /// <summary>
    /// Where a message that can never be saved is kept (forever). "::" is not allowed in ids, so no bucket can take it.
    /// </summary>
    public static string GetTerminatedBucketAddress(string fullBucketAddressId)
    {
        if (string.IsNullOrEmpty(fullBucketAddressId))
        {
            throw new ArgumentNullException(nameof(fullBucketAddressId));
        }

        return $"{JobMasterConstants.ReservedAgentConnectionNamePrefix}Terminated::{fullBucketAddressId}";
    }
}
