using JobMaster.IntegrationTests.Fixtures.RepoConformance;
using Xunit;

namespace JobMaster.IntegrationTests.RepoConformance;

[CollectionDefinition("PostgresRepositoryConformance", DisableParallelization = true)]
public class PostgresRepositoryConformanceCollection : ICollectionFixture<PostgresRepositoryFixture>
{
}
