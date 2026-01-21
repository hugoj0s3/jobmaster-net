using System.ComponentModel;
namespace JobMaster.Sdk.Abstractions.Models;

[EditorBrowsable(EditorBrowsableState.Never)]
public enum AddSavePendingResultCode
{
    Published = 1,
    HeldOnMaster = 2,
    HeldOnMasterNoBucket = 3,
    HeldOnMasterPublishedUnknown = 4,
    PublishFailed = 5,
    AlreadyExists = 6
}

[EditorBrowsable(EditorBrowsableState.Never)]
public enum SaveDrainResultCode
{
    Success = 1,
    Skipped = 2,
    Failed = 3
}