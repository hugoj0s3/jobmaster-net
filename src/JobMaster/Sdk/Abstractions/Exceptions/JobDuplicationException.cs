using System.ComponentModel;
namespace JobMaster.Sdk.Abstractions.Exceptions;

[EditorBrowsable(EditorBrowsableState.Never)]
public sealed class JobDuplicationException : Exception
{
    public Guid JobId { get; }

    public JobDuplicationException(Guid jobId, Exception inner)
        : base($"Job with Id {jobId} already exists.", inner)
    {
        JobId = jobId;
    }
}
