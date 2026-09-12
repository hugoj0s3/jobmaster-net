using Xunit;
// The initialization config, factories happens once per startup. we don't need to unit test parallelization.
[assembly: CollectionBehavior(DisableTestParallelization = true)]
