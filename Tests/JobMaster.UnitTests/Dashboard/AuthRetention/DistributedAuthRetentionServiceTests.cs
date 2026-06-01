using FluentAssertions;
using JobMaster.Dashboard.AuthRetention;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Options;

namespace JobMaster.UnitTests.Dashboard.AuthRetention;

public class DistributedAuthRetentionServiceTests
{
    private readonly IDistributedCache cache =
        new MemoryDistributedCache(Options.Create(new MemoryDistributedCacheOptions()));

    private readonly DistributedAuthRetentionService sut;

    public DistributedAuthRetentionServiceTests()
    {
        sut = new DistributedAuthRetentionService(cache);
    }

    private static StoredAuth NewAuth(string tokenValue = "tok") => new()
    {
        Secrets = new Dictionary<string, string> { ["token"] = tokenValue },
        ExpiresAt = DateTime.UtcNow.AddHours(1)
    };

    [Fact]
    public async Task GetAsync_WhenNotStored_ReturnsNull()
    {
        var result = await sut.GetAsync("session1", "key1");
        result.Should().BeNull();
    }

    [Fact]
    public async Task GetAsync_AfterStore_ReturnsStoredCredentials()
    {
        var stored = NewAuth("abc");
        await sut.StoreAsync("session1", "key1", stored);

        var result = await sut.GetAsync("session1", "key1");

        result.Should().NotBeNull();
        result!.Secrets["token"].Should().Be("abc");
    }

    [Fact]
    public async Task GetAsync_AfterRemove_ReturnsNull()
    {
        await sut.StoreAsync("session1", "key1", NewAuth());
        await sut.RemoveAsync("session1", "key1");

        var result = await sut.GetAsync("session1", "key1");

        result.Should().BeNull();
    }

    [Fact]
    public async Task StoreAsync_DifferentSessions_AreIsolated()
    {
        await sut.StoreAsync("sessionA", "key1", NewAuth("a"));
        await sut.StoreAsync("sessionB", "key1", NewAuth("b"));

        (await sut.GetAsync("sessionA", "key1"))!.Secrets["token"].Should().Be("a");
        (await sut.GetAsync("sessionB", "key1"))!.Secrets["token"].Should().Be("b");
    }

    [Fact]
    public async Task StoreAsync_OverwritesExistingEntry()
    {
        await sut.StoreAsync("session1", "key1", NewAuth("first"));
        await sut.StoreAsync("session1", "key1", NewAuth("second"));

        var result = await sut.GetAsync("session1", "key1");

        result!.Secrets["token"].Should().Be("second");
    }
}
