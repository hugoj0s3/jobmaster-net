using System.Data;
using Dapper;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Exceptions;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.GenericRecords;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;
using JobMaster.Sdk.Utils;
using JobMaster.SqlBase;
using JobMaster.SqlBase.Connections;
using JobMaster.SqlBase.Extensions;
using JobMaster.SqlBase.Master;
using JobMaster.SqlBase.Scripts;

namespace JobMaster.Postgres.Master;

internal class PostgresMasterRecurringSchedulesRepository : SqlMasterRecurringSchedulesRepository
{
    public PostgresMasterRecurringSchedulesRepository(JobMasterClusterConnectionConfig clusterConnectionConfig, IDbConnectionManager connectionManager) : 
        base(clusterConnectionConfig, connectionManager)
    {
    }

    public override string MasterRepoTypeId => PostgresRepositoryConstants.RepositoryTypeId;

    protected override string BuildQueryIdsToLockSql(string whereSql, bool needsMetadataJoin, int countLimit, int offset,
        SortByCriteria? sortByCriteria)
    {
        var baseSql = base.BuildQueryIdsToLockSql(whereSql, needsMetadataJoin, countLimit, offset, sortByCriteria);
        return baseSql + " FOR UPDATE SKIP LOCKED";
    }

}
