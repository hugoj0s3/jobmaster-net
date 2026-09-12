namespace JobMaster.Sdk.Abstractions.Exceptions;

internal interface IKnownExceptionIdentifierStrategy
{
    string RepoType { get; }
    JobMasterKnownExceptionId? Identify(Exception ex);
}
