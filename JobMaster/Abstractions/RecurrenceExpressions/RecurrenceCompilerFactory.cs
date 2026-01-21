namespace JobMaster.Abstractions.RecurrenceExpressions;

public static class RecurrenceCompilerFactory
{
    private static IDictionary<string, IRecurrenceExprCompiler> Compilers =
        new Dictionary<string, IRecurrenceExprCompiler>();
    
    private static bool hasRegistred = false;

    public static IRecurrenceExprCompiler GetCompiler(string recurrenceTypeId)
    {
        if (!hasRegistred)
        {
            AutoRegister();   
        }

        if (!Compilers.TryGetValue(recurrenceTypeId, out var compiler))
            throw new ArgumentException($"Unknown recurrence type: {recurrenceTypeId}");

        return compiler;
    }
    
    public static IRecurrenceExprCompiler? TryGetCompiler(string recurrenceTypeId)
    {
        if (!hasRegistred)
        {
            AutoRegister();   
        }

        if (!Compilers.TryGetValue(recurrenceTypeId, out var compiler))
            return null;

        return compiler;
    }
    
    public static void RegisterCompiler(IRecurrenceExprCompiler compiler, bool replaceIfExists = true)
    {
        if (Compilers.ContainsKey(compiler.ExpressionTypeId) && !replaceIfExists)
        {
            return;
        }
        
        Compilers[compiler.ExpressionTypeId] = compiler;
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