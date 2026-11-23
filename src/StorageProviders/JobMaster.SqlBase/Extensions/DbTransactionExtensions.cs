using System.Data;

namespace JobMaster.SqlBase.Extensions;

internal static class DbTransactionExtensions
{
    /// <summary>
    /// Safely rolls back a transaction only if it is still active (not already committed or rolled back).
    /// This prevents connection leaks that occur when attempting to rollback an already-disposed transaction.
    /// </summary>
    /// <param name="transaction">The transaction to rollback</param>
    public static void SafeRollback(this IDbTransaction? transaction)
    {
        if (transaction?.Connection != null)
        {
            transaction.Rollback();
        }
    }
}
