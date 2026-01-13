using System;
using System.Runtime.CompilerServices;
using JobMaster.Sdk.Contracts.Models.Logs;
using JobMaster.Sdk.Contracts.Services.Master;

namespace JobMaster.Sdk.Contracts.Extensions;

public static class JobMasterLoggerExtensions
{
    public static void Debug(
        this IJobMasterLogger logger,
        string message,
        JobMasterLogSubjectType? subjectType = null,
        string? subjectId = null,
        Exception? exception = null,
        [CallerMemberName] string? sourceMember = null,
        [CallerFilePath] string? sourceFile = null,
        [CallerLineNumber] int sourceLine = 0)
    {
        logger.Log(JobMasterLogLevel.Debug, message, subjectType, subjectId, exception, sourceMember, sourceFile, sourceLine);
    }

    public static void Debug(
        this IJobMasterLogger logger,
        string message,
        JobMasterLogSubjectType subjectType,
        Guid subjectId,
        Exception? exception = null,
        [CallerMemberName] string? sourceMember = null,
        [CallerFilePath] string? sourceFile = null,
        [CallerLineNumber] int sourceLine = 0)
    {
        logger.Debug(message, subjectType, subjectId.ToString("N"), exception, sourceMember, sourceFile, sourceLine);
    }

    public static void Info(
        this IJobMasterLogger logger,
        string message,
        JobMasterLogSubjectType? subjectType = null,
        string? subjectId = null,
        Exception? exception = null,
        [CallerMemberName] string? sourceMember = null,
        [CallerFilePath] string? sourceFile = null,
        [CallerLineNumber] int sourceLine = 0)
    {
        logger.Log(JobMasterLogLevel.Info, message, subjectType, subjectId, exception, sourceMember, sourceFile, sourceLine);
    }

    public static void Info(
        this IJobMasterLogger logger,
        string message,
        JobMasterLogSubjectType subjectType,
        Guid subjectId,
        Exception? exception = null,
        [CallerMemberName] string? sourceMember = null,
        [CallerFilePath] string? sourceFile = null,
        [CallerLineNumber] int sourceLine = 0)
    {
        logger.Info(message, subjectType, subjectId.ToString("N"), exception, sourceMember, sourceFile, sourceLine);
    }

    public static void Warn(
        this IJobMasterLogger logger,
        string message,
        JobMasterLogSubjectType? subjectType = null,
        string? subjectId = null,
        Exception? exception = null,
        [CallerMemberName] string? sourceMember = null,
        [CallerFilePath] string? sourceFile = null,
        [CallerLineNumber] int sourceLine = 0)
    {
        logger.Log(JobMasterLogLevel.Warning, message, subjectType, subjectId, exception, sourceMember, sourceFile, sourceLine);
    }

    public static void Warn(
        this IJobMasterLogger logger,
        string message,
        JobMasterLogSubjectType subjectType,
        Guid subjectId,
        Exception? exception = null,
        [CallerMemberName] string? sourceMember = null,
        [CallerFilePath] string? sourceFile = null,
        [CallerLineNumber] int sourceLine = 0)
    {
        logger.Warn(message, subjectType, subjectId.ToString("N"), exception, sourceMember, sourceFile, sourceLine);
    }

    public static void Error(
        this IJobMasterLogger logger,
        string message,
        JobMasterLogSubjectType? subjectType = null,
        string? subjectId = null,
        Exception? exception = null,
        [CallerMemberName] string? sourceMember = null,
        [CallerFilePath] string? sourceFile = null,
        [CallerLineNumber] int sourceLine = 0)
    {
        logger.Log(JobMasterLogLevel.Error, message, subjectType, subjectId, exception, sourceMember, sourceFile, sourceLine);
    }

    public static void Error(
        this IJobMasterLogger logger,
        string message,
        JobMasterLogSubjectType subjectType,
        Guid subjectId,
        Exception? exception = null,
        [CallerMemberName] string? sourceMember = null,
        [CallerFilePath] string? sourceFile = null,
        [CallerLineNumber] int sourceLine = 0)
    {
        logger.Error(message, subjectType, subjectId.ToString("N"), exception, sourceMember, sourceFile, sourceLine);
    }

    public static void Critical(
        this IJobMasterLogger logger,
        string message,
        JobMasterLogSubjectType? subjectType = null,
        string? subjectId = null,
        Exception? exception = null,
        [CallerMemberName] string? sourceMember = null,
        [CallerFilePath] string? sourceFile = null,
        [CallerLineNumber] int sourceLine = 0)
    {
        logger.Log(JobMasterLogLevel.Critical, message, subjectType, subjectId, exception, sourceMember, sourceFile, sourceLine);
    }

    public static void Critical(
        this IJobMasterLogger logger,
        string message,
        JobMasterLogSubjectType subjectType,
        Guid subjectId,
        Exception? exception = null,
        [CallerMemberName] string? sourceMember = null,
        [CallerFilePath] string? sourceFile = null,
        [CallerLineNumber] int sourceLine = 0)
    {
        logger.Critical(message, subjectType, subjectId.ToString("N"), exception, sourceMember, sourceFile, sourceLine);
    }
}
