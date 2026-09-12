using JobMaster.Sdk.Abstractions.Exceptions;
using MySqlConnector;

namespace JobMaster.MySql.Exceptions;

internal class MySqlKnownExceptionIdentifierStrategy : IKnownExceptionIdentifierStrategy
{
    public string RepoType => MySqlRepositoryConstants.RepositoryTypeId;

    public JobMasterKnownExceptionId? Identify(Exception ex)
    {
        if (ex is MySqlException mysqlEx)
        {
            if (mysqlEx.Number == 1213)
            {
                return JobMasterKnownExceptionId.Deadlock;
            }

            if (mysqlEx.Number == 1062 || mysqlEx.ErrorCode == MySqlErrorCode.DuplicateKeyEntry)
            {
                return JobMasterKnownExceptionId.DuplicateKey;
            }

            if (mysqlEx.Number == 1146 || mysqlEx.ErrorCode == MySqlErrorCode.NoSuchTable)
            {
                return JobMasterKnownExceptionId.SchemaNotProvisioned;
            }
        }

        return null;
    }
}
