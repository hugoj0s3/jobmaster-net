namespace JobMaster.Sdk.Abstractions.Exceptions;

public class JobMasterVersionConflictException : Exception
{
    public Guid Id { get; }
    public string? ExpectedVersion { get; }
    
    public string EntityName { get; }
    
    public JobMasterVersionConflictException(Guid id, string entityName, string? expectedVersion) 
        : base($"{entityName} {id} version conflict. Expected version: {expectedVersion ?? "null"}")
    {
        Id = id;
        EntityName = entityName;
        ExpectedVersion = expectedVersion;
    }
    
    public JobMasterVersionConflictException(Guid id, string entityName, string? expectedVersion, Exception innerException) 
        : base($"{entityName} {id} version conflict. Expected version: {expectedVersion ?? "null"}", innerException)
    {
        Id = id;
        EntityName = entityName;
        ExpectedVersion = expectedVersion;
    }
}
