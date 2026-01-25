namespace JobMaster.Sdk.Abstractions.Background;

internal enum OnBoardingResult
{
    Accepted = 1,
    MovedToMaster,
    TooEarly,
    Invalid,
    Cancelled,
}
