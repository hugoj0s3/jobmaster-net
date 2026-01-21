using JobMaster.IntegrationTests.Fixtures.RepoConformance;
using JobMaster.Sdk.Abstractions.Repositories.Master;
using Xunit;

namespace JobMaster.IntegrationTests.RepoConformance.DistributedLocker;

public abstract class RepositoryDistributedLockerConformanceTests<TFixture>
    where TFixture : class, IRepositoryFixture
{
    protected TFixture Fixture { get; }

    protected RepositoryDistributedLockerConformanceTests(TFixture fixture)
    {
        Fixture = fixture;
    }

    protected IMasterDistributedLockerRepository Repo => Fixture.MasterDistributedLocker;

    [Fact]
    public void TryLock_ShouldReturnToken_And_IsLocked_ShouldBeTrue()
    {
        var key = $"lock-test-{Guid.NewGuid():N}";

        Assert.False(Repo.IsLocked(key));

        var token = Repo.TryLock(key, TimeSpan.FromSeconds(5));
        Assert.False(string.IsNullOrEmpty(token));

        Assert.True(Repo.IsLocked(key));

        Assert.True(Repo.ReleaseLock(key, token!));
        Assert.False(Repo.IsLocked(key));
    }

    [Fact]
    public void TryLock_ShouldBeMutuallyExclusive_UntilReleased()
    {
        var key = $"lock-test-{Guid.NewGuid():N}";

        var token1 = Repo.TryLock(key, TimeSpan.FromSeconds(5));
        Assert.False(string.IsNullOrEmpty(token1));

        var token2 = Repo.TryLock(key, TimeSpan.FromMilliseconds(250));
        Assert.Null(token2);

        Assert.True(Repo.IsLocked(key));

        Assert.True(Repo.ReleaseLock(key, token1!));
        Assert.False(Repo.IsLocked(key));
    }

    [Fact]
    public void ReleaseLock_ShouldRequireCorrectToken()
    {
        var key = $"lock-test-{Guid.NewGuid():N}";

        var token = Repo.TryLock(key, TimeSpan.FromSeconds(5));
        Assert.False(string.IsNullOrEmpty(token));

        Assert.False(Repo.ReleaseLock(key, "wrong-token"));
        Assert.True(Repo.IsLocked(key));

        Assert.True(Repo.ReleaseLock(key, token!));
        Assert.False(Repo.IsLocked(key));
    }

    [Fact]
    public async Task Lock_ShouldExpire_AfterLeaseDuration()
    {
        var key = $"lock-test-{Guid.NewGuid():N}";

        var token1 = Repo.TryLock(key, TimeSpan.FromMilliseconds(200));
        Assert.False(string.IsNullOrEmpty(token1));
        Assert.True(Repo.IsLocked(key));

        await Task.Delay(TimeSpan.FromMilliseconds(450));

        Assert.False(Repo.IsLocked(key));

        var token2 = Repo.TryLock(key, TimeSpan.FromSeconds(5));
        Assert.False(string.IsNullOrEmpty(token2));

        Assert.True(Repo.ReleaseLock(key, token2!));
        Assert.False(Repo.IsLocked(key));
    }
}
