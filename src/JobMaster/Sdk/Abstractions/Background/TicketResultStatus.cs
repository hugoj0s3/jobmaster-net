using System.ComponentModel;
namespace JobMaster.Sdk.Abstractions.Background;

[EditorBrowsable(EditorBrowsableState.Never)]
public enum TicketResultStatus
{
    Success,
    Failed,
    Locked,
    Skipped
}