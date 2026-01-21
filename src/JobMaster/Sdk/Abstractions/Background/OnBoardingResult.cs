namespace JobMaster.Sdk.Abstractions.Background;

public enum OnBoardingResult
{
    Accepted = 1,
    MovedToMaster,
    TooEarly,
    Invalid,
    Cancelled,
}
