namespace JobMaster.Sdk.Abstractions.Config;

/// <summary>
/// A connection config carrying provider-specific options, so providers can read the same option
/// from a cluster (master) or an agent provider config with one extension method.
/// </summary>
internal interface IHasAdditionalConnConfig
{
    JobMasterConfigDictionary AdditionalConnConfig { get; }
}
