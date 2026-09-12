
namespace JobMaster.Api.ApiModels;

/// <summary>Severity level of a log entry.</summary>
public enum ApiJobMasterLogLevel
{
    /// <summary>Verbose diagnostic information.</summary>
    Debug,
    /// <summary>Informational message about normal operation.</summary>
    Info,
    /// <summary>A condition that may require attention but is not an error.</summary>
    Warning,
    /// <summary>An error that impacted an operation.</summary>
    Error,
    /// <summary>A critical failure requiring immediate attention.</summary>
    Critical,
}