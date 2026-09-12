namespace JobMaster.Abstractions;

#if NET7_0_OR_GREATER
/// <summary>
/// Requires a type to expose a static <see cref="JobDefinitionConfig"/>. A <see cref="JobDefinitionConfigAttribute"/>
/// subclass implements this alongside its own <c>public static JobDefinitionConfig Config { get; }</c> to
/// get that member enforced at compile time — the base class can't implement it itself and defer to
/// subclasses, since C# only allows deferring a <c>static abstract</c> interface member through another
/// interface, not through an abstract class. Mirrors <see cref="StaticRecurringSchedules.IStaticRecurringSchedulesProfile"/>'s
/// own use of static abstract interface members.
/// </summary>
public interface IStaticJobDefinitionConfig
{
    static abstract JobDefinitionConfig Config { get; }
}
#else
/// <summary>
/// Requires a type to expose a static <see cref="JobDefinitionConfig"/>. Empty on this target framework —
/// static abstract interface members require net7.0+ (see the net7.0-or-greater build of this file) — so
/// a missing <c>Config</c> is only caught at runtime here, not compile time.
/// </summary>
public interface IStaticJobDefinitionConfig
{
}
#endif
