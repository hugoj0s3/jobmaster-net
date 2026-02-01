
namespace JobMaster.Api.AspNetCore.Auth.ApiKeys;

public interface IApiKeyAuthConfigSelector
{
    /// <summary>
    /// 
    /// </summary>
    /// <param name="ownerName"></param>
    /// <param name="key"></param>
    /// <param name="claims"></param>
    void AddApiKey(string ownerName, string key, IDictionary<string, string>? claims = null);

    /// <summary>
    /// Register a custom API key authentication provider.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    void RegisterApiKeyAuthProvider<T>() where T : class, IJobMasterApiKeyAuthProvider;
    
    /// <summary>
    /// Configure the API key header name.
    /// </summary>
    /// <param name="headerName">The header name to use for API key authentication. Default is "X-Api-Key".</param>
    void ApiKeyHeader(string headerName);
}