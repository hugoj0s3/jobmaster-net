using System.ComponentModel;
namespace JobMaster.Sdk.Abstractions.Background;

[EditorBrowsable(EditorBrowsableState.Never)]
public interface IOnBoardingControl<T>
{
    bool Contains(string id);
    int CountAvailability();
    bool Push(T item, string id, DateTime departureTime, DateTime departureDeadline);
    void ForcePush(T item, string id, DateTime departureTime, DateTime departureDeadline);
    int Count();
    IList<T> PruneDeadlinedItems();
    IList<T> PruneOldDepartureItems(int limit);
    IList<T> GetReadyItems(DateTime now, int limit);
    DateTime? PeekNextDepartureTime();
    IList<T> Shutdown();
}
