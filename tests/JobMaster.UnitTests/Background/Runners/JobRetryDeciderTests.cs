using FluentAssertions;
using JobMaster.Abstractions;
using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Background.Runners.JobsExecution;
using JobMaster.Sdk.Utils;
using Microsoft.Extensions.DependencyInjection;
using Moq;

namespace JobMaster.UnitTests.Background.Runners;

public class JobRetryDeciderTests
{
    [Theory]
    [InlineData(0, 30)]      // first failure → first delay
    [InlineData(1, 120)]
    [InlineData(4, 3600)]    // last built-in delay
    [InlineData(20, 3600)]   // the last one repeats
    public async Task DecideAsync_NoInterceptor_UsesBuiltInDelaysByFailureCount(int failuresBefore, int expectedSeconds)
    {
        var sut = NewDecider(JobMasterRetrySettings.BuiltIn);

        var outcome = await sut.DecideAsync(NewJob(failuresBefore), new Exception("boom"));

        outcome.IsStopEarly.Should().BeFalse();
        outcome.Delay.Should().Be(TimeSpan.FromSeconds(expectedSeconds));
        outcome.Source.Should().Be(JobRetryDelaySource.RetryDelays);
    }

    [Fact]
    public async Task DecideAsync_ClusterRetryDelays_LastOneRepeats()
    {
        var sut = NewDecider(new JobMasterRetrySettings(new[] { TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(7) }, null, null));

        (await sut.DecideAsync(NewJob(0), new Exception())).Delay.Should().Be(TimeSpan.FromMinutes(1));
        (await sut.DecideAsync(NewJob(1), new Exception())).Delay.Should().Be(TimeSpan.FromMinutes(7));
        (await sut.DecideAsync(NewJob(5), new Exception())).Delay.Should().Be(TimeSpan.FromMinutes(7));
    }

    [Fact]
    public async Task DecideAsync_InterceptorDelayUnderMinimum_IsRaisedToMinimum()
    {
        var sut = NewDecider(new JobMasterRetrySettings(null, _ => JobRetryDecision.RetryAfter(TimeSpan.FromSeconds(1)), null));

        var outcome = await sut.DecideAsync(NewJob(0), new Exception());

        outcome.Delay.Should().Be(JobMasterLimits.MinRetryDelay);
        outcome.Source.Should().Be(JobRetryDelaySource.InterceptorBelowMinimum);
    }

    [Fact]
    public async Task DecideAsync_InterceptorStopEarly_StopsWithItsReason()
    {
        var sut = NewDecider(new JobMasterRetrySettings(null,
            ctx => ctx.Exception is ArgumentException ? JobRetryDecision.StopEarly("invalid input") : JobRetryDecision.UseRetryDelays(), null));

        var stopped = await sut.DecideAsync(NewJob(0), new ArgumentException());
        var retried = await sut.DecideAsync(NewJob(0), new InvalidOperationException());

        stopped.IsStopEarly.Should().BeTrue();
        stopped.Reason.Should().Be("invalid input");
        retried.IsStopEarly.Should().BeFalse();
    }

    [Fact]
    public async Task DecideAsync_UseRetryDelays_UsesTheClustersDelays()
    {
        var sut = NewDecider(new JobMasterRetrySettings(
            new[] { TimeSpan.FromMinutes(3) }, _ => JobRetryDecision.UseRetryDelays(), null));

        var outcome = await sut.DecideAsync(NewJob(0), new Exception());

        outcome.Delay.Should().Be(TimeSpan.FromMinutes(3));
        outcome.Source.Should().Be(JobRetryDelaySource.InterceptorUsedRetryDelays);
    }

    [Fact]
    public async Task DecideAsync_InterceptorThrows_StillRetriesWithTheDelays()
    {
        // A broken interceptor must never lose the retry.
        var sut = NewDecider(new JobMasterRetrySettings(
            new[] { TimeSpan.FromMinutes(4) }, _ => throw new InvalidOperationException("broken interceptor"), null));

        var outcome = await sut.DecideAsync(NewJob(0), new Exception());

        outcome.IsStopEarly.Should().BeFalse();
        outcome.Delay.Should().Be(TimeSpan.FromMinutes(4));
        outcome.Source.Should().Be(JobRetryDelaySource.InterceptorFailed);
    }

    [Fact]
    public async Task DecideAsync_Interceptor_SeesThisFailureCountedAndTheException()
    {
        JobRetryContext? seen = null;
        var sut = NewDecider(new JobMasterRetrySettings(null, ctx =>
        {
            seen = ctx;
            return JobRetryDecision.UseRetryDelays();
        }, null));
        var exception = new TimeoutException();

        await sut.DecideAsync(NewJob(0), exception);

        seen!.NumberOfFailures.Should().Be(1);
        seen.Job.NumberOfFailures.Should().Be(1);
        seen.Exception.Should().BeSameAs(exception);
    }

    [Fact]
    public async Task DecideAsync_DiInterceptor_ResolvedFromANewScopeWithScopedDependencies()
    {
        var services = new ServiceCollection();
        services.AddScoped<ScopedDependency>();
        services.AddScoped<DependencyInterceptor>();
        var sut = NewDecider(new JobMasterRetrySettings(null, null, typeof(DependencyInterceptor)), services.BuildServiceProvider());

        var first = await sut.DecideAsync(NewJob(0), new Exception());
        var second = await sut.DecideAsync(NewJob(0), new Exception());

        first.Delay.Should().Be(TimeSpan.FromMinutes(9));
        first.Source.Should().Be(JobRetryDelaySource.Interceptor);
        DependencyInterceptor.SeenDependencies.Should().HaveCount(2);
        DependencyInterceptor.SeenDependencies[0].Should().NotBeSameAs(DependencyInterceptor.SeenDependencies[1]); // a new scope each time
        second.Delay.Should().Be(TimeSpan.FromMinutes(9));
    }

    [Fact]
    public async Task DecideAsync_DiInterceptorTooSlow_IsCancelledAndFallsBackToTheDelays()
    {
        var services = new ServiceCollection();
        services.AddScoped<SlowInterceptor>();
        var sut = NewDecider(
            new JobMasterRetrySettings(new[] { TimeSpan.FromMinutes(2) }, null, typeof(SlowInterceptor)),
            services.BuildServiceProvider(),
            interceptorTimeout: TimeSpan.FromMilliseconds(100));

        var outcome = await sut.DecideAsync(NewJob(0), new Exception());

        outcome.Delay.Should().Be(TimeSpan.FromMinutes(2));
        outcome.Source.Should().Be(JobRetryDelaySource.InterceptorFailed);
        (await SlowInterceptor.TokenCancelled.Task.WaitAsync(TimeSpan.FromSeconds(5))).Should().BeTrue();
    }

    private static JobRetryDecider NewDecider(JobMasterRetrySettings settings, IServiceProvider? serviceProvider = null, TimeSpan? interceptorTimeout = null)
    {
        var clusterConfig = JobMasterClusterConnectionConfig.Create($"retry-{JobMasterRandomUtil.NewGuid4():N}", "repo", "conn");
        clusterConfig.SetRetrySettings(settings);
        return new JobRetryDecider(clusterConfig, serviceProvider ?? new ServiceCollection().BuildServiceProvider(),
            Mock.Of<IJobMasterLogger>(), interceptorTimeout);
    }

    private static JobRawModel NewJob(int numberOfFailures) => new("retry-cluster")
    {
        Id = JobMasterRandomUtil.NewGuid7(),
        JobDefinitionId = "job-def",
        MsgData = "{}",
        NumberOfFailures = numberOfFailures,
        MaxNumberOfRetries = 100,
        ScheduledAt = DateTime.UtcNow,
    };

    private sealed class ScopedDependency
    {
    }

    private sealed class DependencyInterceptor : IJobMasterRetryInterceptor
    {
        public static readonly List<ScopedDependency> SeenDependencies = new();
        private readonly ScopedDependency dependency;

        public DependencyInterceptor(ScopedDependency dependency)
        {
            this.dependency = dependency;
        }

        public Task<JobRetryDecision> DecideAsync(JobRetryContext context, CancellationToken cancellationToken)
        {
            SeenDependencies.Add(dependency);
            return Task.FromResult(JobRetryDecision.RetryAfter(TimeSpan.FromMinutes(9)));
        }
    }

    private sealed class SlowInterceptor : IJobMasterRetryInterceptor
    {
        public static readonly TaskCompletionSource<bool> TokenCancelled = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public async Task<JobRetryDecision> DecideAsync(JobRetryContext context, CancellationToken cancellationToken)
        {
            using var registration = cancellationToken.Register(() => TokenCancelled.TrySetResult(true));
            await Task.Delay(TimeSpan.FromSeconds(30), cancellationToken);
            return JobRetryDecision.RetryAfter(TimeSpan.FromMinutes(1));
        }
    }
}
