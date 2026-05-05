namespace JobMaster.Sdk.Abstractions.Background;

internal interface IOnBoardingControl<T>
{
    int CountAvailability();
    void ForcePush(T item, string id, DateTime departureTime, DateTime departureDeadline);
    IList<T> PullPending(int limit);
    IList<T> GetReadyItems(DateTime now, int limit);
    IList<T> Shutdown();
}
