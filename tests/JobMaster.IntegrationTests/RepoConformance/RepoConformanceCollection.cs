using JobMaster.IntegrationTests.Fixtures.RepoConformance;
using Xunit;

namespace JobMaster.IntegrationTests.RepoConformance;

// One collection for all 3 providers: JobMasterRuntime is a process-wide singleton, so
// RepoConformanceBootstrap (which owns the single StartJobMasterRuntimeAsync() call covering all
// 3 clusters) and the 3 provider fixtures that depend on it must share one collection/lifetime --
// see RepoConformanceBootstrap's class doc for the full reasoning.
[CollectionDefinition("RepoConformance", DisableParallelization = true)]
public class RepoConformanceCollection :
    ICollectionFixture<RepoConformanceBootstrap>,
    ICollectionFixture<PostgresRepositoryFixture>,
    ICollectionFixture<MySqlRepositoryFixture>,
    ICollectionFixture<SqlServerRepositoryFixture>
{
}
