using System.Runtime.CompilerServices;

// Tests
[assembly: InternalsVisibleTo("JobMaster.IntegrationTests")]
[assembly: InternalsVisibleTo("JobMaster.UnitTests")]
[assembly: InternalsVisibleTo("JobMaster.UnitTests.NetStandard")]

// Mocking frameworks
[assembly: InternalsVisibleTo("DynamicProxyGenAssembly2")]
