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
}
