using JobMaster.Sdk.Abstractions.Ioc.Markups;

namespace JobMaster.Sdk.Abstractions.Exceptions;

internal interface IKnownExceptionIdentifier : IJobMasterClusterAwareComponent
{
    JobMasterKnownExceptionId? Identify(Exception ex);
}
