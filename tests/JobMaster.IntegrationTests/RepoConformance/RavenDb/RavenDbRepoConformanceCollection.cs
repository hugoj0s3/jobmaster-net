using JobMaster.IntegrationTests.Fixtures.RepoConformance;
using Xunit;

namespace JobMaster.IntegrationTests.RepoConformance.RavenDb;

// Separate from "RepoConformance" (the 3 SQL providers' shared collection) -- RavenDbRepositoryFixture
// never calls StartJobMasterRuntimeAsync(), so it has no process-wide-singleton coordination need with
// the SQL bootstrap. See RavenDbRepositoryFixture's class doc for the full reasoning.
[CollectionDefinition("RavenDbRepoConformance", DisableParallelization = true)]
public class RavenDbRepoConformanceCollection : ICollectionFixture<RavenDbRepositoryFixture>
{
}
