using System;
using System.Collections.Generic;
using System.Linq;
using JobMaster.Abstractions.Serialization;

namespace JobMaster.Serialization;

/// <summary>
/// An <see cref="IJobMasterMessageJsonSerializer"/> that writes with one serializer and reads with several,
/// so values written in an older format keep working after you change serializer.
/// <para>
/// Writes always use <c>primary</c>. Reads try <c>primary</c>, then each fallback in order, and return the first
/// result that doesn't throw.
/// </para>
/// <para>
/// A fallback is only tried when a serializer <b>throws</b>. A serializer that reads data in another format without
/// throwing (for example, <c>System.Text.Json</c> reading camelCase JSON into PascalCase properties returns an object
/// with empty properties) stops the chain with a wrong result. Configure each serializer to reject data it doesn't
/// own, for example <c>System.Text.Json</c> with <c>UnmappedMemberHandling = JsonUnmappedMemberHandling.Disallow</c>.
/// </para>
/// <para>
/// To switch serializer without downtime, deploy in two steps: first every node with
/// <c>new FallbackJobMasterMessageJsonSerializer(oldSerializer, newSerializer)</c> (still writing the old format,
/// already reading the new one), then, once every node runs it,
/// <c>new FallbackJobMasterMessageJsonSerializer(newSerializer, oldSerializer)</c>.
/// </para>
/// </summary>
public sealed class FallbackJobMasterMessageJsonSerializer : IJobMasterMessageJsonSerializer
{
    private readonly IJobMasterMessageJsonSerializer primary;
    private readonly IReadOnlyList<IJobMasterMessageJsonSerializer> readers;

    /// <summary>Creates a serializer that writes with <paramref name="primary"/> and reads with it, then with <paramref name="fallbacks"/> in order.</summary>
    /// <param name="primary">Used for every write, and tried first on reads.</param>
    /// <param name="fallbacks">Tried in order on reads when the previous serializer throws.</param>
    public FallbackJobMasterMessageJsonSerializer(IJobMasterMessageJsonSerializer primary, params IJobMasterMessageJsonSerializer[] fallbacks)
    {
        if (primary is null) throw new ArgumentNullException(nameof(primary));
        if (fallbacks is null) throw new ArgumentNullException(nameof(fallbacks));
        if (fallbacks.Any(f => f is null)) throw new ArgumentNullException(nameof(fallbacks), "Fallback serializers cannot contain null.");

        this.primary = primary;
        readers = new[] { primary }.Concat(fallbacks).ToArray();
    }

    /// <inheritdoc />
    public string Serialize<T>(T obj) => primary.Serialize(obj);

    /// <inheritdoc />
    public T Deserialize<T>(string json)
    {
        var failures = new List<string>();
        foreach (var reader in readers)
        {
            try
            {
                return reader.Deserialize<T>(json);
            }
            catch (Exception ex)
            {
                failures.Add(Describe(reader, ex));
            }
        }

        throw AllFailed(typeof(T), failures);
    }

    /// <inheritdoc />
    public object Deserialize(string json, Type type)
    {
        var failures = new List<string>();
        foreach (var reader in readers)
        {
            try
            {
                return reader.Deserialize(json, type);
            }
            catch (Exception ex)
            {
                failures.Add(Describe(reader, ex));
            }
        }

        throw AllFailed(type, failures);
    }

    private static string Describe(IJobMasterMessageJsonSerializer reader, Exception ex) => $"{reader.GetType().Name}: {ex.GetType().Name}";

    // Inner exceptions are not attached: some serializers echo the offending payload in their message.
    private static InvalidOperationException AllFailed(Type type, List<string> failures)
        => new($"No serializer could deserialize the value as {type.Name} ({string.Join(", ", failures)}).");
}
