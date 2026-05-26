using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using Dapper;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.GenericRecords;
using JobMaster.SqlBase.Connections;
using JobMaster.Sdk.Abstractions.Exceptions;
using JobMaster.SqlBase;
using JobMaster.SqlBase.Extensions;
using JobMaster.SqlBase.Master;
using JobMaster.SqlBase.Scripts;

namespace JobMaster.Postgres.Master;

internal class PostgresMasterJobsRepository : SqlMasterJobsRepository
{
    public PostgresMasterJobsRepository(JobMasterClusterConnectionConfig clusterConnectionConfig,
        IDbConnectionManager connectionManager,
        IKnownExceptionIdentifier knownExceptionIdentifier) :
        base(clusterConnectionConfig, connectionManager, knownExceptionIdentifier)
    {
    }

    public override string MasterRepoTypeId => PostgresRepositoryConstants.RepositoryTypeId;

    protected override string BuildQueryIdsToLockSql(string whereSql, bool needsMetadataJoin, int countLimit, int offset,
        SortByCriteria? sortByCriteria)
    {
        var baseSql = base.BuildQueryIdsToLockSql(whereSql, needsMetadataJoin, countLimit, offset, sortByCriteria);
        return baseSql + " FOR UPDATE SKIP LOCKED";
    }


    private string BuildJobUpsertSql()
    {
        var t = TableName();
        var (cols, vals) = InsertColumnsAndParams();
        var conflictCols = $"{Col(x => x.ClusterId)}, {Col(x => x.Id)}";
        var cVersion = Col(x => x.Version);
        return $@"
INSERT INTO {t} ({cols}) VALUES ({vals})
ON CONFLICT ({conflictCols}) DO UPDATE SET
    {UpdateSetClause()}
WHERE {t}.{cVersion} = @ExpectedVersion;";
    }
}