using JobMaster.IntegrationTests.Fixtures.RepoConformance;
using Xunit;

namespace JobMaster.IntegrationTests.RepoConformance;

[CollectionDefinition("SqlServerRepositoryConformance", DisableParallelization = true)]
public class SqlServerRepositoryConformanceCollection : ICollectionFixture<SqlServerRepositoryFixture>
{
}
