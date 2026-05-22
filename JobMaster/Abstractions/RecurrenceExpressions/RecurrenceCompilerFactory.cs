using System.Collections.Concurrent;

namespace JobMaster.Abstractions.RecurrenceExpressions;

/// <summary>
/// Registry that maps recurrence type identifiers to their <see cref="IRecurrenceExprCompiler"/> implementations.
/// All compilers in the assembly are auto-registered on first access. Use <see cref="RegisterCompiler"/>
/// to add or override compilers for custom recurrence types.
/// </summary>
public static class RecurrenceCompilerFactory
{
    private static ConcurrentDictionary<string, IRecurrenceExprCompiler> Compilers =
        new ConcurrentDictionary<string, IRecurrenceExprCompiler>();

    private static bool hasRegistred = false;
    private static object locker = new object();

    /// <summary>
    /// Returns the compiler registered for <paramref name="recurrenceTypeId"/>.
    /// Throws <see cref="ArgumentException"/> if no compiler is found.
    /// </summary>
    public static IRecurrenceExprCompiler GetCompiler(string recurrenceTypeId)
    {
        EnsureIsRegistred();

        if (!Compilers.TryGetValue(recurrenceTypeId, out var compiler))
            throw new ArgumentException($"Unknown recurrence type: {recurrenceTypeId}");

        return compiler;
    }

    /// <summary>
    /// Returns the compiler registered for <paramref name="recurrenceTypeId"/>,
    /// or <c>null</c> if none is found.
    /// </summary>
    public static IRecurrenceExprCompiler? TryGetCompiler(string recurrenceTypeId)
    {
        EnsureIsRegistred();

        if (!Compilers.TryGetValue(recurrenceTypeId, out var compiler))
            return null;

        return compiler;
    }

    /// <summary>
    /// Registers <paramref name="compiler"/> under its <see cref="IRecurrenceExprCompiler.ExpressionTypeId"/>.
    /// If <paramref name="replaceIfExists"/> is <c>false</c>, an existing registration for the same type ID
    /// is kept and the new compiler is ignored.
    /// </summary>
    public static void RegisterCompiler(IRecurrenceExprCompiler compiler, bool replaceIfExists = true)
    {
        if (Compilers.ContainsKey(compiler.ExpressionTypeId) && !replaceIfExists)
        {
            return;
        }

        Compilers[compiler.ExpressionTypeId] = compiler;
    }

    private static void EnsureIsRegistred()
    {
        lock (locker)
        {
            if (!hasRegistred)
            {
                AutoRegister();
            }
        }
    }

    private static void AutoRegister()
    {
        var types = typeof(IRecurrenceExprCompiler).Assembly.GetTypes()
            .Where(t => typeof(IRecurrenceExprCompiler).IsAssignableFrom(t) && !t.IsInterface);

        foreach (var type in types)
        {
            var instance = (IRecurrenceExprCompiler)Activator.CreateInstance(type)!;
            RegisterCompiler(instance, replaceIfExists: false);
        }

        hasRegistred = true;
    }
}
