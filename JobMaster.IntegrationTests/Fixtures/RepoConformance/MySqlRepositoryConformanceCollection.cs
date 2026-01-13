using Xunit;

namespace JobMaster.IntegrationTests.Fixtures.RepoConformance;

[CollectionDefinition("MySqlRepositoryConformance")]
public sealed class MySqlRepositoryConformanceCollection : ICollectionFixture<MySqlRepositoryFixture>
{
}
