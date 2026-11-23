using JobMaster.Abstractions.RecurrenceExpressions;
using Microsoft.Extensions.DependencyInjection;

namespace JobMaster.NCrontab;

public static class JobMasterNCrontabServiceCollectionExtensions
{
    /// <summary>
    /// Registers the NCrontab recurrence compiler (<see cref="NCrontabExprCompiler.TypeId"/>). Unlike the built-in
    /// <c>TimeSpanInterval</c>/<c>NaturalCron</c> compilers, NCrontab ships as a separate assembly and is not
    /// auto-discovered by <see cref="RecurrenceCompilerFactory"/> -- call this before <c>AddJobMasterCluster</c>.
    /// </summary>
    public static IServiceCollection AddJobMasterNCrontab(this IServiceCollection services)
    {
        RecurrenceCompilerFactory.RegisterCompiler(new NCrontabExprCompiler());
        return services;
    }
}
