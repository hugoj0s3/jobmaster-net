using System.Reflection;
using FluentAssertions;
using JobMaster.Sdk.Utils;

namespace JobMaster.UnitTests.Sdk.Utils;

public class TypeAttributeCacheExtensionsTests
{
    [Fact]
    public void GetCachedAttribute_ReturnsSameValueAsReflection()
    {
        typeof(Decorated).GetCachedAttribute<LabelAttribute>()!.Label
            .Should().Be(typeof(Decorated).GetCustomAttribute<LabelAttribute>()!.Label);
    }

    [Fact]
    public void GetCachedAttribute_ReturnsSameInstanceOnEveryCall()
    {
        var first = typeof(Decorated).GetCachedAttribute<LabelAttribute>();
        var second = typeof(Decorated).GetCachedAttribute<LabelAttribute>();

        second.Should().BeSameAs(first);
    }

    [Fact]
    public void GetCachedAttribute_WhenMissing_ReturnsNull()
    {
        typeof(Plain).GetCachedAttribute<LabelAttribute>().Should().BeNull();
    }

    [Fact]
    public void GetCachedAttribute_RespectsInherit()
    {
        typeof(Derived).GetCachedAttribute<LabelAttribute>(inherit: true)!.Label.Should().Be("base");
        typeof(Derived).GetCachedAttribute<LabelAttribute>(inherit: false).Should().BeNull();
    }

    [Fact]
    public void GetCachedAttribute_WhenMultipleMatch_ReturnsOneInsteadOfThrowing()
    {
        // Deliberately more lenient than GetCustomAttribute<T>() (AmbiguousMatchException): rules against multiples
        // are enforced at startup (DefaultRuntimeValidatorSetup), not on the per-job path.
        var cached = () => typeof(TwoMarkers).GetCachedAttribute<MarkerBaseAttribute>();

        cached.Should().NotThrow();
        cached().Should().NotBeNull();
    }

    [Fact]
    public void GetCachedAttribute_DoesNotInstantiateUnrelatedAttributes()
    {
        // Regression: caching every attribute on the type surfaced ThrowingAttribute's constructor exception through
        // an unrelated lookup -- GetJobHandlerTypeFromId scans every handler, so one bad handler broke all jobs.
        var lookup = () => typeof(WithThrowingAttribute).GetCachedAttribute<LabelAttribute>();

        lookup.Should().NotThrow();
        lookup()!.Label.Should().Be("ok");
    }

    [Fact]
    public void GetCachedAttributes_ReturnsAllMatches()
    {
        typeof(TwoMarkers).GetCachedAttributes<MarkerBaseAttribute>().Should().HaveCount(2);
    }

    [AttributeUsage(AttributeTargets.Class, Inherited = true)]
    private sealed class LabelAttribute(string label) : Attribute
    {
        public string Label { get; } = label;
    }

    [AttributeUsage(AttributeTargets.Class)]
    private abstract class MarkerBaseAttribute : Attribute;

    private sealed class MarkerOneAttribute : MarkerBaseAttribute;

    private sealed class MarkerTwoAttribute : MarkerBaseAttribute;

    [Label("decorated")]
    private sealed class Decorated;

    private sealed class Plain;

    [Label("base")]
    private class Base;

    private sealed class Derived : Base;

    [MarkerOne]
    [MarkerTwo]
    private sealed class TwoMarkers;

    [AttributeUsage(AttributeTargets.Class)]
    private sealed class ThrowingAttribute : Attribute
    {
        public ThrowingAttribute() => throw new InvalidOperationException("must not be instantiated");
    }

    [Label("ok")]
    [Throwing]
    private sealed class WithThrowingAttribute;
}
