using JobMaster.Sdk.Abstractions.Exceptions;
using Npgsql;

namespace JobMaster.Postgres.Exceptions;

internal class PostgresKnownExceptionIdentifierStrategy : IKnownExceptionIdentifierStrategy
{
    public string RepoType => PostgresRepositoryConstants.RepositoryTypeId;

    public JobMasterKnownExceptionId? Identify(Exception ex)
    {
        if (ex is PostgresException pgEx)
        {
            switch (pgEx.SqlState)
            {
                case "40P01":
                    return JobMasterKnownExceptionId.Deadlock;
                case "23505":
                    return JobMasterKnownExceptionId.DuplicateKey;
                case "42P01":
                    return JobMasterKnownExceptionId.SchemaNotProvisioned;
            }
        }

        return null;
    }
}
