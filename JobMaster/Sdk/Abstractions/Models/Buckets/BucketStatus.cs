using System.ComponentModel;
namespace JobMaster.Sdk.Abstractions.Models.Buckets;

[EditorBrowsable(EditorBrowsableState.Never)]
public enum BucketStatus
{
    Active = 1,     // Active and accepting new jobs
    Completing = 2,   // Still processing existing jobs but not accepting new ones
    
    ReadyToDrain = 3, // Ready to be drained, but not assigned to any worker
    Draining = 4,     // Being emptied by cleanup worker
    
    Lost = 5,         // Worker died and bucket is orphaned
    
    ReadyToDelete = 6, // Ready to be deleted. all jobs are completed or drained. just waiting for deletion confirmation.
}
