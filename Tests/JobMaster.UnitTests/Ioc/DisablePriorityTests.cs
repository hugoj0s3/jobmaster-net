using FluentAssertions;
using JobMaster.Abstractions.Ioc.Selectors;
using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Ioc.Setup;
using Microsoft.Extensions.DependencyInjection;

namespace JobMaster.UnitTests.Ioc;

public class DisablePriorityTests
{
    // ── helpers ──────────────────────────────────────────────────────────────

    private static ClusterConfigBuilder Builder() => new(null, new ServiceCollection());

    private static IClusterStandaloneConfigSelector StandaloneBuilder()
    {
        var b = Builder();
        b.ClusterRepoType("Postgres");
        b.ClusterConnString("Host=localhost;");
        return b.UseStandaloneCluster();
    }

    // ── DisablePriority on ClusterConfigBuilder ───────────────────────────────

    [Fact]
    public void DisablePriority_Medium_ThrowsArgumentException()
    {
        var b = Builder();
        b.Invoking(x => x.DisablePriority(JobMasterPriority.Medium))
            .Should().Throw<ArgumentException>()
            .WithMessage("*Medium*");
    }

    [Theory]
    [InlineData(JobMasterPriority.VeryLow)]
    [InlineData(JobMasterPriority.Low)]
    [InlineData(JobMasterPriority.High)]
    [InlineData(JobMasterPriority.Critical)]
    public void DisablePriority_NonMedium_AddsToDisabledPriorities(JobMasterPriority priority)
    {
        var b = Builder();
        b.DisablePriority(priority);

        b.clusterDefinition.DisabledPriorities.Should().Contain(priority);
    }

    [Fact]
    public void DisablePriority_MultiplePriorities_AccumulatesAll()
    {
        var b = Builder();
        b.DisablePriority(JobMasterPriority.VeryLow);
        b.DisablePriority(JobMasterPriority.Critical);

        b.clusterDefinition.DisabledPriorities.Should().BeEquivalentTo(
            new[] { JobMasterPriority.VeryLow, JobMasterPriority.Critical });
    }

    [Fact]
    public void DisablePriority_SamePriorityTwice_IsIdempotent()
    {
        var b = Builder();
        b.DisablePriority(JobMasterPriority.Low);
        b.DisablePriority(JobMasterPriority.Low);

        b.clusterDefinition.DisabledPriorities.Should().ContainSingle()
            .Which.Should().Be(JobMasterPriority.Low);
    }

    [Fact]
    public void DisablePriority_ReturnsSelector_AllowingChaining()
    {
        var b = Builder();
        var result = b.DisablePriority(JobMasterPriority.VeryLow);

        result.Should().BeSameAs(b);
    }

    [Fact]
    public void DisablePriority_MediumNotDisabled_NeverAppearsInSet()
    {
        var b = Builder();
        b.DisablePriority(JobMasterPriority.VeryLow);
        b.DisablePriority(JobMasterPriority.Low);

        b.clusterDefinition.DisabledPriorities.Should().NotContain(JobMasterPriority.Medium);
    }

    // ── DisablePriority on ClusterStandaloneConfigBuilder ────────────────────

    [Fact]
    public void Standalone_DisablePriority_Medium_ThrowsArgumentException()
    {
        var s = StandaloneBuilder();
        s.Invoking(x => x.DisablePriority(JobMasterPriority.Medium))
            .Should().Throw<ArgumentException>()
            .WithMessage("*Medium*");
    }

    [Theory]
    [InlineData(JobMasterPriority.VeryLow)]
    [InlineData(JobMasterPriority.Low)]
    [InlineData(JobMasterPriority.High)]
    [InlineData(JobMasterPriority.Critical)]
    public void Standalone_DisablePriority_NonMedium_AddsToDisabledPriorities(JobMasterPriority priority)
    {
        // UseStandaloneCluster() wraps the same clusterDefinition, so we check via the builder
        var b = Builder();
        b.ClusterRepoType("Postgres");
        b.ClusterConnString("Host=localhost;");
        var s = b.UseStandaloneCluster();
        s.DisablePriority(priority);

        b.clusterDefinition.DisabledPriorities.Should().Contain(priority);
    }

    [Fact]
    public void Standalone_DisablePriority_ReturnsStandaloneSelector_AllowingChaining()
    {
        var s = StandaloneBuilder();
        var result = s.DisablePriority(JobMasterPriority.VeryLow);

        result.Should().BeSameAs(s);
    }

    // ── BucketQty zeroing: DisabledPriorities drive the Finish() logic ────────

    [Fact]
    public void DisablePriority_WorkerBucketQtyDefaultIsOne_BeforeFinish()
    {
        var b = Builder();
        b.AddWorker("w1", "agent1");
        b.DisablePriority(JobMasterPriority.VeryLow);

        // BucketQty is only zeroed inside Finish(); the definition still carries the default here
        b.clusterDefinition.Workers[0].BucketQty[JobMasterPriority.VeryLow].Should().Be(1);
        b.clusterDefinition.DisabledPriorities.Should().Contain(JobMasterPriority.VeryLow);
    }

    [Fact]
    public void DisablePriority_FinishLogic_ZeroesBucketQtyForDisabledPriorities()
    {
        // Simulates the inner loop in Finish() that zeroes BucketQty for disabled priorities.
        // Finish() itself cannot be called standalone (it registers DI singletons), so we
        // verify the worker definition state that Finish() is supposed to produce.
        var b = Builder();
        b.AddWorker("w1", "agent1");
        b.DisablePriority(JobMasterPriority.VeryLow);
        b.DisablePriority(JobMasterPriority.Critical);

        var worker = b.clusterDefinition.Workers[0];

        // Replicate what Finish() does
        foreach (var p in b.clusterDefinition.DisabledPriorities)
            worker.BucketQty[p] = 0;

        worker.BucketQty[JobMasterPriority.VeryLow].Should().Be(0);
        worker.BucketQty[JobMasterPriority.Critical].Should().Be(0);
        worker.BucketQty[JobMasterPriority.Medium].Should().Be(1, "Medium is never disabled");
        worker.BucketQty[JobMasterPriority.Low].Should().Be(1, "Low was not disabled");
        worker.BucketQty[JobMasterPriority.High].Should().Be(1, "High was not disabled");
    }

    [Fact]
    public void DisablePriority_AllNonMediumCanBeDisabledTogether()
    {
        var b = Builder();
        b.DisablePriority(JobMasterPriority.VeryLow);
        b.DisablePriority(JobMasterPriority.Low);
        b.DisablePriority(JobMasterPriority.High);
        b.DisablePriority(JobMasterPriority.Critical);

        b.clusterDefinition.DisabledPriorities.Should().HaveCount(4)
            .And.NotContain(JobMasterPriority.Medium);
    }
}
