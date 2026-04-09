using JobMaster.IntegrationTests.Fixtures.SchedulerFixture.Mixed;
using JobMaster.IntegrationTests.Fixtures.SchedulerFixture.MySqlPure;
using JobMaster.IntegrationTests.Fixtures.SchedulerFixture.NatsJetStream;
using JobMaster.IntegrationTests.Fixtures.SchedulerFixture.PostgresPure;
using JobMaster.IntegrationTests.Fixtures.SchedulerFixture.SqlServerPure;
using Xunit.Abstractions;

namespace JobMaster.IntegrationTests.ScheduleTests;

[Collection("NatsJetStreamDrainMode")]
public class NatsJetStreamDrainModeTests : JobMasterSchedulerTestsBase<NatsJetStreamDrainModeFixture>
{
    public NatsJetStreamDrainModeTests(NatsJetStreamDrainModeFixture fixture, ITestOutputHelper output) : base(fixture, output) { }

    [Theory]
    [InlineData(100000, 20, 30)]
    [InlineData(300000, 60, 90)]
    [InlineData(1000000, 100, 180)]
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
    [InlineData(100000, 40, 60)]
    [InlineData(300000, 120, 180)]
    [InlineData(1000000, 200, 360)]
    public async Task DrainModeTest(int qtyJobs, int timeoutInMinutes, int secondsToStopWorkers)
    {
        await RunDrainModeTest(qtyJobs, timeoutInMinutes, secondsToStopWorkers, scheduleParallelLimit:50);
    }
}

[Collection("SqlServerDrainMode")]
public class SqlServerDrainModeTests : JobMasterSchedulerTestsBase<SqlServerDrainModeFixture>
{
    public SqlServerDrainModeTests(SqlServerDrainModeFixture fixture, ITestOutputHelper output) : base(fixture, output) { }

    [Theory]
    [InlineData(100000, 40, 60)]
    [InlineData(300000, 120, 180)]
    [InlineData(1000000, 200, 360)]
    public async Task DrainModeTest(int qtyJobs, int timeoutInMinutes, int secondsToStopWorkers)
    {
        await RunDrainModeTest(qtyJobs, timeoutInMinutes, secondsToStopWorkers, scheduleParallelLimit:50);
    }
}

[Collection("MySqlDrainMode")]
public class MySqlDrainModeTests : JobMasterSchedulerTestsBase<MySqlDrainModeFixture>
{
    public MySqlDrainModeTests(MySqlDrainModeFixture fixture, ITestOutputHelper output) : base(fixture, output) { }

    [Theory]
    [InlineData(50000, 40, 60)]
    [InlineData(150000, 120, 180)]
    [InlineData(300000, 220, 360)]
    public async Task DrainModeTest(int qtyJobs, int timeoutInMinutes, int secondsToStopWorkers)
    {
        await RunDrainModeTest(qtyJobs, timeoutInMinutes, secondsToStopWorkers, scheduleParallelLimit:25);
    }
}

[Collection("MixedDrainMode")]
public class MixedDrainModeTests : JobMasterSchedulerTestsBase<MixedDrainModeFixture>
{
    public MixedDrainModeTests(MixedDrainModeFixture fixture, ITestOutputHelper output) : base(fixture, output) { }

    [Theory]
    [InlineData(100000, 40, 60)]
    [InlineData(300000, 120, 180)]
    [InlineData(1000000, 200, 360)]
    public async Task DrainModeTest(int qtyJobs, int timeoutInMinutes, int secondsToStopWorkers)
    {
        await RunDrainModeTest(qtyJobs, timeoutInMinutes, secondsToStopWorkers, scheduleParallelLimit:50);
    }
}
