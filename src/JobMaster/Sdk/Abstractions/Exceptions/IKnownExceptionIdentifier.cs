using JobMaster.Sdk.Abstractions.Ioc.Markups;

namespace JobMaster.Sdk.Abstractions.Exceptions;

internal interface IKnownExceptionIdentifier : IJobMasterClusterAwareComponent
{
    JobMasterKnownExceptionId? Identify(string repoType, Exception ex);

    /// <summary>
    /// Identifies an exception without knowing which repo type threw it — tries every
    /// strategy registered for this cluster and returns the first match. Safe because the raw
    /// provider exception types each strategy pattern-matches on (<c>SqlException</c>,
    /// <c>PostgresException</c>, <c>MySqlException</c>, ...) are mutually exclusive CLR types,
    /// so there's no real ambiguity to resolve by repo type — and cheap, since a cluster only
    /// ever registers a handful of strategies. Use this when the caller has no single connection
    /// in scope (e.g. a runner's generic tick-failure classifier); prefer
    /// <see cref="Identify(string, Exception)"/> whenever the repo type is actually known.
    /// </summary>
    JobMasterKnownExceptionId? Identify(Exception ex);
}
