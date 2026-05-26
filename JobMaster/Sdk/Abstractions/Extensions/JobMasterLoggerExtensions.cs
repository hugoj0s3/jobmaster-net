using System.Runtime.CompilerServices;
using JobMaster.Sdk.Abstractions.Models.Logs;
using JobMaster.Sdk.Abstractions.Services.Master;

namespace JobMaster.Sdk.Abstractions.Extensions;

internal static class JobMasterLoggerExtensions
{
    public static void Debug(
        this IJobMasterLogger logger,
        string message,
        JobMasterLogCategory? category = null,
        string? referenceId = null,
        Exception? exception = null,
        [CallerMemberName] string? sourceMember = null,
        [CallerFilePath] string? sourceFile = null,
        [CallerLineNumber] int sourceLine = 0)
    {
        logger.Log(JobMasterLogLevel.Debug, message, category, referenceId, exception, sourceMember, sourceFile, sourceLine);
    }

    public static void Debug(
        this IJobMasterLogger logger,
        string message,
        JobMasterLogCategory category,
        Guid referenceId,
        Exception? exception = null,
        [CallerMemberName] string? sourceMember = null,
        [CallerFilePath] string? sourceFile = null,
        [CallerLineNumber] int sourceLine = 0)
    {
        logger.Debug(message, category, referenceId.ToString("N"), exception, sourceMember, sourceFile, sourceLine);
    }

    public static void Info(
        this IJobMasterLogger logger,
        string message,
        JobMasterLogCategory? category = null,
        string? referenceId = null,
        Exception? exception = null,
        [CallerMemberName] string? sourceMember = null,
        [CallerFilePath] string? sourceFile = null,
        [CallerLineNumber] int sourceLine = 0)
    {
        logger.Log(JobMasterLogLevel.Info, message, category, referenceId, exception, sourceMember, sourceFile, sourceLine);
    }

    public static void Info(
        this IJobMasterLogger logger,
        string message,
        JobMasterLogCategory category,
        Guid referenceId,
        Exception? exception = null,
        [CallerMemberName] string? sourceMember = null,
        [CallerFilePath] string? sourceFile = null,
        [CallerLineNumber] int sourceLine = 0)
    {
        logger.Info(message, category, referenceId.ToString("N"), exception, sourceMember, sourceFile, sourceLine);
    }

    public static void Warn(
        this IJobMasterLogger logger,
        string message,
        JobMasterLogCategory? category = null,
        string? referenceId = null,
        Exception? exception = null,
        [CallerMemberName] string? sourceMember = null,
        [CallerFilePath] string? sourceFile = null,
        [CallerLineNumber] int sourceLine = 0)
    {
        logger.Log(JobMasterLogLevel.Warning, message, category, referenceId, exception, sourceMember, sourceFile, sourceLine);
    }

    public static void Warn(
        this IJobMasterLogger logger,
        string message,
        JobMasterLogCategory category,
        Guid referenceId,
        Exception? exception = null,
        [CallerMemberName] string? sourceMember = null,
        [CallerFilePath] string? sourceFile = null,
        [CallerLineNumber] int sourceLine = 0)
    {
        logger.Warn(message, category, referenceId.ToString("N"), exception, sourceMember, sourceFile, sourceLine);
    }

    public static void Error(
        this IJobMasterLogger logger,
        string message,
        JobMasterLogCategory? category = null,
        string? referenceId = null,
        Exception? exception = null,
        [CallerMemberName] string? sourceMember = null,
        [CallerFilePath] string? sourceFile = null,
        [CallerLineNumber] int sourceLine = 0)
    {
        logger.Log(JobMasterLogLevel.Error, message, category, referenceId, exception, sourceMember, sourceFile, sourceLine);
    }

    public static void Error(
        this IJobMasterLogger logger,
        string message,
        JobMasterLogCategory category,
        Guid referenceId,
        Exception? exception = null,
        [CallerMemberName] string? sourceMember = null,
        [CallerFilePath] string? sourceFile = null,
        [CallerLineNumber] int sourceLine = 0)
    {
        logger.Error(message, category, referenceId.ToString("N"), exception, sourceMember, sourceFile, sourceLine);
    }

    public static void Critical(
        this IJobMasterLogger logger,
        string message,
        JobMasterLogCategory? category = null,
        string? referenceId = null,
        Exception? exception = null,
        [CallerMemberName] string? sourceMember = null,
        [CallerFilePath] string? sourceFile = null,
        [CallerLineNumber] int sourceLine = 0)
    {
        logger.Log(JobMasterLogLevel.Critical, message, category, referenceId, exception, sourceMember, sourceFile, sourceLine);
    }

    public static void Critical(
        this IJobMasterLogger logger,
        string message,
        JobMasterLogCategory category,
        Guid referenceId,
        Exception? exception = null,
        [CallerMemberName] string? sourceMember = null,
        [CallerFilePath] string? sourceFile = null,
        [CallerLineNumber] int sourceLine = 0)
    {
        logger.Critical(message, category, referenceId.ToString("N"), exception, sourceMember, sourceFile, sourceLine);
    }
}
