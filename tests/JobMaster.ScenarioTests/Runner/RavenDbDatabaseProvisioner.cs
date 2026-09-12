using Raven.Client.Documents;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;

namespace JobMaster.ScenarioTests.Runner;

internal static class RavenDbDatabaseProvisioner
{
    public static async Task CreateDatabasesIfNotExistsAsync(string ravenDbUrl, IEnumerable<string> databaseNames, CancellationToken ct = default)
    {
        using var store = new DocumentStore { Urls = [ravenDbUrl] };
        store.Initialize();

        var existingNames = await store.Maintenance.Server.SendAsync(new GetDatabaseNamesOperation(0, int.MaxValue), ct);
        var existing = new HashSet<string>(existingNames, StringComparer.OrdinalIgnoreCase);

        foreach (var databaseName in databaseNames)
        {
            if (existing.Contains(databaseName))
            {
                continue;
            }

            await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord(databaseName)), ct);
        }
    }
}
