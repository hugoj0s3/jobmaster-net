using JobMaster.Sdk.Abstractions.Exceptions;
using Microsoft.Data.SqlClient;

namespace JobMaster.SqlServer.Exceptions;

internal class SqlServerKnownExceptionIdentifierStrategy : IKnownExceptionIdentifierStrategy
{
    public string RepoType => SqlServerRepositoryConstants.RepositoryTypeId;

    public JobMasterKnownExceptionId? Identify(Exception ex)
    {
        if (ex is SqlException sqlEx)
        {
            switch (sqlEx.Number)
            {
                case 1205:
                    return JobMasterKnownExceptionId.Deadlock;
                case 2627:
                    return JobMasterKnownExceptionId.DuplicateKey;
            }
        }

        return null;
    }
}
