using JobMaster.IntegrationTests.Fixtures.SchedulerFixture.Mixed;
using JobMaster.IntegrationTests.Fixtures.SchedulerFixture.MySqlPure;
using JobMaster.IntegrationTests.Fixtures.SchedulerFixture.NatJetStream;
using JobMaster.IntegrationTests.Fixtures.SchedulerFixture.PostgresPure;
using JobMaster.IntegrationTests.Fixtures.SchedulerFixture.SqlServerPure;
using Xunit.Abstractions;

namespace JobMaster.IntegrationTests.ScheduleTests;

[Collection("NatJetStreamDrainMode")]
public class NatJetStreamDrainModeTests : JobMasterSchedulerTestsBase<NatJetStreamDrainModeFixture>
{
    public NatJetStreamDrainModeTests(NatJetStreamDrainModeFixture fixture, ITestOutputHelper output) : base(fixture, output) { }

    [Theory]
    [InlineData(100000, 10, 30)]
    [InlineData(300000, 30, 30)]
    [InlineData(1000000, 50, 30)]
    public async Task DrainModeTest(int qtyJobs, int timeoutInMinutes, int secondsToStopWorkers)
    {
        await RunDrainModeTest(qtyJobs, timeoutInMinutes, secondsToStopWorkers);
    }
}

[Collection("PostgresDrainMode")]
public class PostgresDrainModeTests : JobMasterSchedulerTestsBase<PostgresDrainModeFixture>
{
    public PostgresDrainModeTests(PostgresDrainModeFixture fixture, ITestOutputHelper output) : base(fixture, output) { }

    [Theory]
    [InlineData(100000, 10, 30)]
    [InlineData(300000, 30, 30)]
    [InlineData(1000000, 50, 30)]
    public async Task DrainModeTest(int qtyJobs, int timeoutInMinutes, int secondsToStopWorkers)
    {
        await RunDrainModeTest(qtyJobs, timeoutInMinutes, secondsToStopWorkers);
    }
}

[Collection("SqlServerDrainMode")]
public class SqlServerDrainModeTests : JobMasterSchedulerTestsBase<SqlServerDrainModeFixture>
{
    public SqlServerDrainModeTests(SqlServerDrainModeFixture fixture, ITestOutputHelper output) : base(fixture, output) { }

    [Theory]
    [InlineData(100000, 10, 30)]
    [InlineData(300000, 30, 30)]
    [InlineData(1000000, 50, 30)]
    public async Task DrainModeTest(int qtyJobs, int timeoutInMinutes, int secondsToStopWorkers)
    {
        await RunDrainModeTest(qtyJobs, timeoutInMinutes, secondsToStopWorkers, 3);
    }
}

[Collection("MySqlDrainMode")]
public class MySqlDrainModeTests : JobMasterSchedulerTestsBase<MySqlDrainModeFixture>
{
    public MySqlDrainModeTests(MySqlDrainModeFixture fixture, ITestOutputHelper output) : base(fixture, output) { }

    [Theory]
    [InlineData(100000, 10, 30)]
    [InlineData(300000, 30, 30)]
    [InlineData(1000000, 50, 30)]
    public async Task DrainModeTest(int qtyJobs, int timeoutInMinutes, int secondsToStopWorkers)
    {
        await RunDrainModeTest(qtyJobs, timeoutInMinutes, secondsToStopWorkers);
    }
}

[Collection("MixedDrainMode")]
public class MixedDrainModeTests : JobMasterSchedulerTestsBase<MixedDrainModeFixture>
{
    public MixedDrainModeTests(MixedDrainModeFixture fixture, ITestOutputHelper output) : base(fixture, output) { }

    [Theory]
    [InlineData(100000, 10, 30)]
    [InlineData(300000, 30, 30)]
    [InlineData(1000000, 50, 30)]
    public async Task DrainModeTest(int qtyJobs, int timeoutInMinutes, int secondsToStopWorkers)
    {
        await RunDrainModeTest(qtyJobs, timeoutInMinutes, secondsToStopWorkers);
    }
}
