namespace JobMaster.Sdk.Abstractions.Exceptions;

internal enum JobMasterKnownExceptionId
{
    Deadlock = 1,
    VersionConflict = 2,
    DuplicateKey = 3,
    SchemaNotProvisioned = 4,
}
