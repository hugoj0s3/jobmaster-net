using JobMaster.Abstractions.RecurrenceExpressions;
using Microsoft.Extensions.DependencyInjection;

namespace JobMaster.Cronos;

public static class JobMasterCronosServiceCollectionExtensions
{
    /// <summary>
    /// Registers the Cronos recurrence compiler (<see cref="CronosExprCompiler.TypeId"/>). Unlike the built-in
    /// <c>TimeSpanInterval</c>/<c>NaturalCron</c> compilers, Cronos ships as a separate assembly and is not
    /// auto-discovered by <see cref="RecurrenceCompilerFactory"/> -- call this before <c>AddJobMasterCluster</c>.
    /// </summary>
    public static IServiceCollection AddJobMasterCronos(this IServiceCollection services)
    {
        RecurrenceCompilerFactory.RegisterCompiler(new CronosExprCompiler());
        return services;
    }
}
