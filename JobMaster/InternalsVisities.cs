using System.Runtime.CompilerServices;
// Tests.
[assembly: InternalsVisibleTo("JobMaster.UnitTests")]
[assembly: InternalsVisibleTo("JobMaster.IntegrationTests")]

// Mocking frameworks
[assembly: InternalsVisibleTo("DynamicProxyGenAssembly2")]

// Projects
[assembly: InternalsVisibleTo("JobMaster.SqlBase")]
[assembly: InternalsVisibleTo("JobMaster.Postgres")]
[assembly: InternalsVisibleTo("JobMaster.MySql")]
[assembly: InternalsVisibleTo("JobMaster.SqlServer")]
[assembly: InternalsVisibleTo("JobMaster.NatsJetStream")]
