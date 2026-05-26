namespace JobMaster.Sdk.Abstractions.Background;

/// <summary>
/// Holds jobs that have been accepted by the engine but are not yet ready to enter the
/// execution queue. Items are kept in a sorted holding pen ordered by departure time
/// (ascending) and are released in two different ways:
/// <list type="bullet">
///   <item><description>
///     <see cref="GetReadyItems"/> — dequeues from the <b>front</b> (earliest departure
///     time) for items whose departure time has passed. Called on every engine pulse to
///     feed the <see cref="ITaskQueueControl{T}"/>.
///   </description></item>
///   <item><description>
///     <see cref="PullPending"/> — dequeues from the <b>back</b> (latest departure time)
///     for bulk operations such as flushing all staged items back to the master during a
///     bucket drain or shutdown.
///   </description></item>
/// </list>
/// </summary>
/// <typeparam name="T">The job model type held by this control.</typeparam>
internal interface IOnBoardingControl<T>
{
    /// <summary>
    /// Returns the number of additional items that can be accepted before the holding
    /// pen reaches capacity (<c>capacity - current count</c>).
    /// </summary>
    int CountAvailability();

    /// <summary>
    /// Returns the number of items currently sitting in the holding pen.
    /// </summary>
    int CountItems();

    /// <summary>
    /// Adds or replaces an item in the holding pen (upsert semantics).
    /// </summary>
    /// <remarks>
    /// <para>
    /// If an item with the same <paramref name="id"/> already exists it is removed first,
    /// then the new entry is inserted — effectively updating its departure time.
    /// </para>
    /// <para>
    /// Items are kept sorted by <paramref name="departureTime"/> in ascending order using
    /// binary search insertion (O(log n)), so <see cref="GetReadyItems"/> can efficiently
    /// read from the front without scanning.
    /// </para>
    /// <para>
    /// No-op when the control is shutting down.
    /// </para>
    /// </remarks>
    /// <param name="item">The job value to hold.</param>
    /// <param name="id">Unique identifier used for deduplication and lookup.</param>
    /// <param name="departureTime">The UTC time at or after which this item is ready
    /// to be released by <see cref="GetReadyItems"/>.</param>
    void Push(T item, string id, DateTime departureTime);

    /// <summary>
    /// Pulls up to <paramref name="limit"/> items from the <b>back</b> of the holding pen
    /// (latest departure times first), removing them from tracking.
    /// </summary>
    /// <remarks>
    /// Used for bulk flush operations — e.g. moving all staged jobs back to master during
    /// a bucket drain — where chronological order is not required.
    /// Returns an empty list when the control is shutting down.
    /// </remarks>
    IList<T> PullPending(int limit);

    /// <summary>
    /// Returns up to <paramref name="limit"/> items from the <b>front</b> of the holding
    /// pen (earliest departure times first) whose <c>departureTime &lt;= <paramref name="now"/></c>,
    /// removing them from tracking.
    /// </summary>
    /// <remarks>
    /// Stops as soon as the next item in the sorted list is not yet due, so only a single
    /// pass over the head of the list is needed. Returns an empty list when the control is
    /// shutting down.
    /// </remarks>
    /// <param name="now">Current UTC time used as the readiness threshold.</param>
    /// <param name="limit">Maximum number of items to dequeue in one call.</param>
    IList<T> GetReadyItems(DateTime now, int limit);

    /// <summary>
    /// Returns <see langword="true"/> if an item with the given <paramref name="id"/> is
    /// currently tracked in the holding pen.
    /// </summary>
    bool Contains(string id);

    /// <summary>
    /// Returns a snapshot of all item IDs currently tracked in the holding pen.
    /// </summary>
    IList<string> GetIds();

    /// <summary>
    /// Initiates shutdown: drains the holding pen and returns all remaining items, then
    /// prevents any further <see cref="Push"/>, <see cref="PullPending"/>, or
    /// <see cref="GetReadyItems"/> calls from doing work.
    /// </summary>
    /// <returns>All items that were in the holding pen at the time of shutdown.</returns>
    IList<T> Shutdown();
}
