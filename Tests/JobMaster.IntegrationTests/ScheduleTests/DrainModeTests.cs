using JobMaster.IntegrationTests.Fixtures.SchedulerFixture.Mixed;
using JobMaster.IntegrationTests.Fixtures.SchedulerFixture.MySqlPure;
using JobMaster.IntegrationTests.Fixtures.SchedulerFixture.NatsJetStream;
using JobMaster.IntegrationTests.Fixtures.SchedulerFixture.PostgresPure;
using JobMaster.IntegrationTests.Fixtures.SchedulerFixture.SqlServerPure;
using Xunit.Abstractions;

namespace JobMaster.IntegrationTests.ScheduleTests;

[Collection("NatsJetStreamDrainMode")]
[Trait("DB", "Nats")]
public class NatsJetStreamDrainModeTests : JobMasterSchedulerTestsBase<NatsJetStreamDrainModeFixture>
{
    public NatsJetStreamDrainModeTests(NatsJetStreamDrainModeFixture fixture, ITestOutputHelper output) : base(fixture, output) { }

    [Theory]
    [Trait("TestType", "DrainMode")]
    [InlineData(10000, 20, 15)]
    [InlineData(100000, 60, 60)]
    public async Task DrainModeTest(int qtyJobs, int timeoutInMinutes, int secondsToStopWorkers)
    {
        await RunDrainModeTest(qtyJobs, timeoutInMinutes, secondsToStopWorkers, scheduleParallelLimit:50);
    }
}

[Collection("PostgresDrainMode")]
[Trait("DB", "Postgres")]
public class PostgresDrainModeTests : JobMasterSchedulerTestsBase<PostgresDrainModeFixture>
{
    public PostgresDrainModeTests(PostgresDrainModeFixture fixture, ITestOutputHelper output) : base(fixture, output) { }

    [Theory]
    [Trait("TestType", "DrainMode")]
    [InlineData(10000, 20, 15)]
    [InlineData(100000, 60, 60)]
    public async Task DrainModeTest(int qtyJobs, int timeoutInMinutes, int secondsToStopWorkers)
    {
        await RunDrainModeTest(qtyJobs, timeoutInMinutes, secondsToStopWorkers, scheduleParallelLimit:50);
    }
}

[Collection("SqlServerDrainMode")]
[Trait("DB", "SqlServer")]
public class SqlServerDrainModeTests : JobMasterSchedulerTestsBase<SqlServerDrainModeFixture>
{
    public SqlServerDrainModeTests(SqlServerDrainModeFixture fixture, ITestOutputHelper output) : base(fixture, output) { }

    [Theory]
    [Trait("TestType", "DrainMode")]
    [InlineData(10000, 20, 15)]
    [InlineData(100000, 60, 60)]
    public async Task DrainModeTest(int qtyJobs, int timeoutInMinutes, int secondsToStopWorkers)
    {
        await RunDrainModeTest(qtyJobs, timeoutInMinutes, secondsToStopWorkers, scheduleParallelLimit:3);
    }
}

[Collection("MySqlDrainMode")]
[Trait("DB", "MySql")]
public class MySqlDrainModeTests : JobMasterSchedulerTestsBase<MySqlDrainModeFixture>
{
    public MySqlDrainModeTests(MySqlDrainModeFixture fixture, ITestOutputHelper output) : base(fixture, output) { }

    [Theory]
    [Trait("TestType", "DrainMode")]
    [InlineData(10000, 20, 15)]
    [InlineData(100000, 60, 60)]
    public async Task DrainModeTest(int qtyJobs, int timeoutInMinutes, int secondsToStopWorkers)
    {
        await RunDrainModeTest(qtyJobs, timeoutInMinutes, secondsToStopWorkers, scheduleParallelLimit:25);
    }
}

[Collection("MixedDrainMode")]
[Trait("DB", "Mixed")]
public class MixedDrainModeTests : JobMasterSchedulerTestsBase<MixedDrainModeFixture>
{
    public MixedDrainModeTests(MixedDrainModeFixture fixture, ITestOutputHelper output) : base(fixture, output) { }

    [Theory]
    [Trait("TestType", "DrainMode")]
    [InlineData(10000, 20, 15)]
    [InlineData(100000, 60, 60)]
    public async Task DrainModeTest(int qtyJobs, int timeoutInMinutes, int secondsToStopWorkers)
    {
        await RunDrainModeTest(qtyJobs, timeoutInMinutes, secondsToStopWorkers, scheduleParallelLimit:50);
    }
}
