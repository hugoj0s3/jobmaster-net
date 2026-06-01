
namespace JobMaster.Api.AspNetCore.Auth.ApiKeys;

/// <summary>
/// Fluent configuration for API key authentication on the JobMaster HTTP API.
/// Use this selector to register one or more fixed API keys or a custom key provider.
/// All methods return void; call them in sequence during DI setup.
/// </summary>
public interface IApiKeyAuthConfigSelector
{
    /// <summary>
    /// Registers a fixed API key with an optional set of claims.
    /// When a request presents this key, the specified claims are attached to its identity.
    /// </summary>
    /// <param name="ownerName">A descriptive label for the key owner (e.g. "service-account", "admin").</param>
    /// <param name="key">The secret API key value that callers must supply in the request header.</param>
    /// <param name="claims">Optional dictionary of claim name/value pairs to attach to the authenticated identity.</param>
    IApiKeyAuthConfigSelector AddApiKey(string ownerName, string key, IDictionary<string, string>? claims = null);

    /// <summary>
    /// Registers a custom API key authentication provider.
    /// The provider is resolved from the DI container, giving you full control over
    /// key lookup, validation, and claims mapping.
    /// </summary>
    /// <typeparam name="T">A class that implements <see cref="IJobMasterApiKeyAuthProvider"/> and is registered in DI.</typeparam>
    IApiKeyAuthConfigSelector RegisterApiKeyAuthProvider<T>() where T : class, IJobMasterApiKeyAuthProvider;

    /// <summary>
    /// Overrides the HTTP header name from which the API key is read.
    /// Defaults to <c>X-Api-Key</c>.
    /// </summary>
    /// <param name="headerName">The custom header name to use for API key extraction.</param>
    IApiKeyAuthConfigSelector ApiKeyHeader(string headerName);
}
