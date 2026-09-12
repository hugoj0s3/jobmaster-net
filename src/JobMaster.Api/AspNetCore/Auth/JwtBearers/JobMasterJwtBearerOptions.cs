using Microsoft.IdentityModel.Tokens;

namespace JobMaster.Api.AspNetCore.Auth.JwtBearers;

internal sealed class JobMasterJwtBearerOptions
{
    public string AuthorizationHeaderName { get; set; } = "Authorization";

    public string Scheme { get; set; } = "Bearer";
    
    internal TokenValidationParameters? TokenValidationParameters { get; set; }

    internal Type? JwtBearerAuthProviderType { get; set; }
}
