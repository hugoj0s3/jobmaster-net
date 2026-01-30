namespace JobMaster.Api.AspNetCore.Auth.JwtBearers;

public interface IJobMasterJwtBearerAuthProvider
{
    Task<JobMasterJwtBearerIdentity?> ValidateTokenAsync(string token);
    
    string GenerateToken(JobMasterJwtBearerIdentity identity, TimeSpan? lifetime = null);
}

