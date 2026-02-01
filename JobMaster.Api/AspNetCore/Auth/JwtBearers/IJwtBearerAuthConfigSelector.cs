using Microsoft.IdentityModel.Tokens;

namespace JobMaster.Api.AspNetCore.Auth.JwtBearers;

public interface IJwtBearerAuthConfigSelector
{
    void RegisterJwtBearerAuthProvider<T>() where T : class, IJobMasterJwtBearerAuthProvider;
    
    void RegisterDefaultJwtBearerAuthProvider(TokenValidationParameters parameters);

    void AuthorizationHeaderName(string headerName);

    void Scheme(string scheme);
}