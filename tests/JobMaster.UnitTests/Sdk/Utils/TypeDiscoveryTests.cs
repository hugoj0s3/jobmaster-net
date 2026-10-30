using System.Reflection;
using FluentAssertions;
using JobMaster.Sdk.Utils;

namespace JobMaster.UnitTests.Sdk.Utils;

public class TypeDiscoveryTests
{
    [Fact]
    public void Scans_ReadEachAssemblyOnce_AcrossRepeatedCalls()
    {
        var assembly = new FakeAssembly(typeof(ConcreteA), typeof(AbstractB));

        TypeDiscovery.AllTypes(new[] { assembly }).ToList();
        TypeDiscovery.AllTypes(new[] { assembly }).ToList();
        typeof(IMarker).GetCachedConcreteTypesOf(new[] { assembly }).ToList();
        typeof(IMarker).GetCachedConcreteTypesOf(new[] { assembly }).ToList();

        assembly.GetTypesCalls.Should().Be(1);
    }

    [Fact]
    public void Scans_PickUpAnAssemblyLoadedAfterTheFirstCall()
    {
        var first = new FakeAssembly(typeof(ConcreteA));
        var later = new FakeAssembly(typeof(ConcreteC));

        TypeDiscovery.AllTypes(new[] { first }).Should().Equal(typeof(ConcreteA));

        TypeDiscovery.AllTypes(new Assembly[] { first, later }).Should().Equal(typeof(ConcreteA), typeof(ConcreteC));
        first.GetTypesCalls.Should().Be(1);
    }

    [Fact]
    public void TypesOf_WhenSomeTypesFailToLoad_KeepsTheLoadableOnes()
    {
        var assembly = new FakeAssembly(new ReflectionTypeLoadException(
            new Type?[] { typeof(ConcreteA), null }!, new Exception?[] { null, new TypeLoadException("missing dependency") }!));

        TypeDiscovery.TypesOf(assembly).Should().Equal(typeof(ConcreteA));
    }

    [Fact]
    public void TypesOf_WhenTheAssemblyCantEnumerateTypes_ReturnsNone()
    {
        var assembly = new FakeAssembly(new NotSupportedException());

        TypeDiscovery.TypesOf(assembly).Should().BeEmpty();
    }

    [Fact]
    public void GetCachedConcreteTypesOf_SkipsAbstractTypesAndInterfaces()
    {
        var assembly = new FakeAssembly(typeof(ConcreteA), typeof(AbstractB), typeof(IMarker), typeof(ConcreteC));

        typeof(IMarker).GetCachedConcreteTypesOf(new[] { assembly }).Should().Equal(typeof(ConcreteA));
    }

    private interface IMarker;

    private sealed class ConcreteA : IMarker;

    private abstract class AbstractB : IMarker;

    private sealed class ConcreteC;

    // A new instance per test: the cache is keyed by assembly, so tests never share entries.
    private sealed class FakeAssembly : Assembly
    {
        private readonly Type[] types = Array.Empty<Type>();
        private readonly Exception? exception;

        public FakeAssembly(params Type[] types) => this.types = types;

        public FakeAssembly(Exception exception) => this.exception = exception;

        public int GetTypesCalls { get; private set; }

        public override Type[] GetTypes()
        {
            GetTypesCalls++;
            return exception is null ? types : throw exception;
        }
    }
}
