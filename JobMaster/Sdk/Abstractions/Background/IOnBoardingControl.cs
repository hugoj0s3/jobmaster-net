namespace JobMaster.Sdk.Abstractions.Background;

internal interface IOnBoardingControl<T>
{
    int CountAvailability();
    void Push(T item, string id, DateTime departureTime);
    IList<T> PullPending(int limit);
    IList<T> GetReadyItems(DateTime now, int limit);
    IList<T> Shutdown();
}
