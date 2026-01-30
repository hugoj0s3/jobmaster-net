
namespace JobMaster.Api.AspNetCore.Auth.ApiKeys;

public interface IApiKeyAuthConfigSelector
{
    void AddApiKey(string key, string ownerName, IDictionary<string, string>? claims = null);

    void RegisterApiKeyAuthProvider<T>() where T : class, IJobMasterApiKeyAuthProvider;
    
    void ApiKeyHeader(string headerName);
}