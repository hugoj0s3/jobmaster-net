namespace JobMaster.Api.AspNetCore.Auth.ApiKeys;

public interface IJobMasterApiKeyAuthProvider 
{
    Task<JobMasterApiKeyIdentity?> GetApiKeyIdentityAsync(string key);
}