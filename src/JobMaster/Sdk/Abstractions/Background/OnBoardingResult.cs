using System.ComponentModel;
namespace JobMaster.Sdk.Abstractions.Background;

[EditorBrowsable(EditorBrowsableState.Never)]
public enum OnBoardingResult
{
    Accepted = 1,
    MovedToMaster,
    TooEarly,
    Invalid,
    Cancelled,
}
