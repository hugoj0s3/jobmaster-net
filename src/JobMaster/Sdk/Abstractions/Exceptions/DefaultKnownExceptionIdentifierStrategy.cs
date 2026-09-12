namespace JobMaster.Sdk.Abstractions.Exceptions;

internal class DefaultKnownExceptionIdentifierStrategy : IKnownExceptionIdentifierStrategy
{
    // Default is never dispatched through the dictionary; router injects it separately
    // as the fallback. RepoType is unused here.
    public string RepoType => string.Empty;

    public JobMasterKnownExceptionId? Identify(Exception ex) => null;
}
