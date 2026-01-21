#if JOBMASTER
namespace JobMaster.Internals;
#elif NATJS
namespace JobMaster.NatJetStream.Internals.Utils;
#elif SQLPROV
namespace JobMaster.Sql.Internals.Utils;
#else
namespace JobMaster.Internal.Utils;
#endif

internal static class CollectionExtensions
{
    internal static bool IsNullOrEmpty<T>(this ICollection<T>? list) => list == null || !list.Any();
    
    internal static T? Random<T>(this IList<T> list)
    {
        if (list.IsNullOrEmpty())
        {
            return default;
        }
        
        // Directly access the index. 
        // No .ElementAt(), no .ToList(), no allocations.
        var index = JobMasterRandomUtil.GetInt(list.Count);
        return list[index];
    }
    
    internal static IEnumerable<ICollection<T>> Partition<T>(this ICollection<T> list, int partitionSize)
    {
        if (partitionSize < 2)
        {
            throw new ArgumentOutOfRangeException(
                nameof(partitionSize),
                partitionSize,
                "Must be greater or equal to 2.");
        }

        T[]? partition = null;
        var count = 0;

        using (var e = list.GetEnumerator())
        {
            if (e.MoveNext())
            {
                partition = new T[partitionSize];
                partition[0] = e.Current;
                count = 1;
            }
            else
            {
                yield break;
            }

            while (e.MoveNext())
            {
                partition[count] = e.Current;
                count++;

                if (count == partitionSize)
                {
                    yield return partition;
                    count = 0;
                    partition = new T[partitionSize];
                }
            }
        }

        if (count > 0)
        {
            Array.Resize(ref partition, count);
            yield return partition;
        }
    }
    
#if !NET6_0_OR_GREATER
    internal static TSource? MinBy<TSource, TKey>(
        this IEnumerable<TSource> source,
        Func<TSource, TKey> keySelector,
        IComparer<TKey>? comparer = null)
    {
        if (source == null) throw new ArgumentNullException(nameof(source));
        if (keySelector == null) throw new ArgumentNullException(nameof(keySelector));
        comparer ??= Comparer<TKey>.Default;

        using var e = source.GetEnumerator();
        if (!e.MoveNext()) return default;

        var minElem = e.Current;
        var minKey = keySelector(minElem);
        while (e.MoveNext())
        {
            var cur = e.Current;
            var key = keySelector(cur);
            if (comparer.Compare(key, minKey) < 0)
            {
                minKey = key;
                minElem = cur;
            }
        }
        return minElem;
    }

    // Finds the element with the maximum key (O(n))
    internal static TSource? MaxBy<TSource, TKey>(
        this IEnumerable<TSource> source,
        Func<TSource, TKey> keySelector,
        IComparer<TKey>? comparer = null)
    {
        if (source == null) throw new ArgumentNullException(nameof(source));
        if (keySelector == null) throw new ArgumentNullException(nameof(keySelector));
        comparer ??= Comparer<TKey>.Default;

        using var e = source.GetEnumerator();
        if (!e.MoveNext()) return default;

        var maxElem = e.Current;
        var maxKey = keySelector(maxElem);
        while (e.MoveNext())
        {
            var cur = e.Current;
            var key = keySelector(cur);
            if (comparer.Compare(key, maxKey) > 0)
            {
                maxKey = key;
                maxElem = cur;
            }
        }
        return maxElem;
    }
#endif
}