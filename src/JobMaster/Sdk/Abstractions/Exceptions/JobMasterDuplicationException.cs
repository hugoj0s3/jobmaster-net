namespace JobMaster.Sdk.Abstractions.Exceptions;

internal sealed class JobMasterDuplicationException : Exception
{
    public Guid Id { get; }
    public string EntityName { get; }

    public JobMasterDuplicationException(Guid id, string entityName, Exception inner)
        : base($"Job with Id {id} already exists.", inner)
    {
        Id = id;
        EntityName = entityName;
    }
}
