namespace JobMaster.Sdk.Abstractions.Exceptions;

internal sealed class JobDuplicationException : Exception
{
    public Guid JobId { get; }

    public JobDuplicationException(Guid jobId, Exception inner)
        : base($"Job with Id {jobId} already exists.", inner)
    {
        JobId = jobId;
    }
}
