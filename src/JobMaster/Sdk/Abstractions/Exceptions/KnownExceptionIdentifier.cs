using JobMaster.Sdk.Abstractions.Config;

namespace JobMaster.Sdk.Abstractions.Exceptions;

internal class KnownExceptionIdentifier : IKnownExceptionIdentifier
{
    private readonly IReadOnlyDictionary<string, IKnownExceptionIdentifierStrategy> byRepoType;
    private readonly IKnownExceptionIdentifierStrategy defaultStrategy;

    public JobMasterClusterConnectionConfig ClusterConnConfig { get; }

    public KnownExceptionIdentifier(
        JobMasterClusterConnectionConfig clusterConnConfig,
        IEnumerable<IKnownExceptionIdentifierStrategy> strategies,
        DefaultKnownExceptionIdentifierStrategy defaultStrategy)
    {
        this.ClusterConnConfig = clusterConnConfig;
        // The same repo type can be registered more than once (e.g. it's used for both the
        // cluster's master connection and one of its agent connections) — keep the first
        // registration per repo type instead of throwing, since every registration for a given
        // repo type is the same stateless strategy implementation anyway.
        this.byRepoType = strategies
            .GroupBy(s => s.RepoType, StringComparer.OrdinalIgnoreCase)
            .ToDictionary(g => g.Key, g => g.First(), StringComparer.OrdinalIgnoreCase);
        this.defaultStrategy = defaultStrategy;
    }

    public JobMasterKnownExceptionId? Identify(string repoType, Exception ex)
    {
        switch (ex)
        {
            case JobMasterDuplicationException:
                return JobMasterKnownExceptionId.DuplicateKey;
            case JobMasterVersionConflictException:
                return JobMasterKnownExceptionId.VersionConflict;
        }

        if (byRepoType.TryGetValue(repoType, out var strategy))
        {
            return strategy.Identify(ex);
        }

        return defaultStrategy.Identify(ex);
    }

    public JobMasterKnownExceptionId? Identify(Exception ex)
    {
        switch (ex)
        {
            case JobMasterDuplicationException:
                return JobMasterKnownExceptionId.DuplicateKey;
            case JobMasterVersionConflictException:
                return JobMasterKnownExceptionId.VersionConflict;
        }
        
        foreach (var strategy in byRepoType.Values)
        {
            var result = strategy.Identify(ex);
            if (result is not null) return result;
        }

        return defaultStrategy.Identify(ex);
    }
}
