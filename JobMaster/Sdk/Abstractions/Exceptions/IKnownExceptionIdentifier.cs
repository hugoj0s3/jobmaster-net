namespace JobMaster.Sdk.Abstractions.Exceptions;

internal interface IKnownExceptionIdentifier
{
    JobMasterKnownExceptionId? Identify(string repoType, Exception ex);
}
