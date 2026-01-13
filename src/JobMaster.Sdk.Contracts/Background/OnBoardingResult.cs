namespace JobMaster.Sdk.Contracts.Background;

public enum OnBoardingResult
{
    Accepted = 1,
    MovedToMaster,
    TooEarly,
    Invalid,
    Cancelled,
}
