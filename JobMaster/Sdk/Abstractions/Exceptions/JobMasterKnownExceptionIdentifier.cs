using JobMaster.Sdk.Abstractions.Config;

namespace JobMaster.Sdk.Abstractions.Exceptions;

internal class JobMasterKnownExceptionIdentifier : IKnownExceptionIdentifier
{
    private readonly IReadOnlyDictionary<string, IKnownExceptionIdentifierStrategy> byRepoType;
    private readonly IKnownExceptionIdentifierStrategy defaultStrategy;

    public JobMasterClusterConnectionConfig ClusterConnConfig { get; }

    public JobMasterKnownExceptionIdentifier(
        JobMasterClusterConnectionConfig clusterConnConfig,
        IEnumerable<IKnownExceptionIdentifierStrategy> strategies,
        DefaultKnownExceptionIdentifierStrategy defaultStrategy)
    {
        this.ClusterConnConfig = clusterConnConfig;
        this.byRepoType = strategies.ToDictionary(s => s.RepoType, StringComparer.OrdinalIgnoreCase);
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
}
