using System.Security.Claims;
using JobMaster.Sdk.Utils.Extensions;
using Microsoft.Extensions.Options;
using Microsoft.IdentityModel.JsonWebTokens;
using Microsoft.IdentityModel.Tokens;

namespace JobMaster.Api.AspNetCore.Auth.JwtBearers;

internal sealed class JobMasterDefaultJwtAuthProvider : IJobMasterJwtBearerAuthProvider
{
    private readonly IOptions<JobMasterApiOptions> options;
    public JobMasterDefaultJwtAuthProvider(IOptions<JobMasterApiOptions> options)
    {
        this.options = options;
    }

    public async Task<JobMasterJwtBearerIdentity?> ValidateTokenAsync(string token)
    {
        var parameters = options.Value?.JwtBearerOptions?.TokenValidationParameters;
        if (parameters == null)
        {
            return null;
        }
        
        var handler = new JsonWebTokenHandler();
        
        // Modern async validation
        var result = await handler.ValidateTokenAsync(token, parameters);

        if (!result.IsValid) return null;

        // Extract the Subject (sub)
        var subject = result.ClaimsIdentity.Name 
                      ?? result.ClaimsIdentity.Claims.FirstOrDefault(c => c.Type == "sub")?.Value;

        if (string.IsNullOrWhiteSpace(subject)) return null;

        return new JobMasterJwtBearerIdentity
        {
            Subject = subject,
            Claims = result.ClaimsIdentity.Claims.ToDictionary(c => c.Type, c => c.Value)
        };
    }
    
    public string GenerateToken(JobMasterJwtBearerIdentity identity, TimeSpan? lifetime = null)
    {
        var parameters = options.Value?.JwtBearerOptions?.TokenValidationParameters;
        if (parameters == null) throw new InvalidOperationException("TokenValidationParameters not configured.");
        
        var signingKey = GetSigningKey(parameters);
    
        if (signingKey == null)
        {
            throw new InvalidOperationException("No signing key found in TokenValidationParameters. Check IssuerSigningKey or IssuerSigningKeys.");
        }

        var handler = new JsonWebTokenHandler();
    
        var algorithm = signingKey is SymmetricSecurityKey 
            ? SecurityAlgorithms.HmacSha256Signature 
            : SecurityAlgorithms.RsaSha256;

        var descriptor = new SecurityTokenDescriptor
        {
            Issuer = parameters.ValidIssuer,
            Audience = parameters.ValidAudience,
            Expires = DateTime.UtcNow.Add(lifetime ?? TimeSpan.FromHours(1)),
            Subject = new ClaimsIdentity(new[] { new Claim(JwtRegisteredClaimNames.Sub, identity.Subject) }),
            Claims = identity.Claims.ToDictionary(c => c.Key, c => (object)c.Value),
            SigningCredentials = new SigningCredentials(signingKey, algorithm)
        };

        return handler.CreateToken(descriptor);
    }
    
    private SecurityKey? GetSigningKey(TokenValidationParameters parameters)
    {
        if (parameters.IssuerSigningKey != null) return parameters.IssuerSigningKey;

        if (parameters.IssuerSigningKeys?.Any() == true) return parameters.IssuerSigningKeys.ToList().Random();

        if (parameters.IssuerSigningKeyResolver != null)
        {
            return parameters.IssuerSigningKeyResolver(string.Empty, null, string.Empty, parameters)?.FirstOrDefault();
        }

        return null;
    }
}