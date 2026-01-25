using System.Runtime.CompilerServices;
// Tests.
[assembly: InternalsVisibleTo("JobMaster.UnitTests")]
[assembly: InternalsVisibleTo("JobMaster.IntegrationTests")]


// Projects
[assembly: InternalsVisibleTo("JobMaster.SqlBase")]
[assembly: InternalsVisibleTo("JobMaster.Postgres")]
[assembly: InternalsVisibleTo("JobMaster.NatsJetStream")]
