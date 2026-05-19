using Microsoft.IdentityModel.Tokens;

namespace JobMaster.Api.AspNetCore.Auth.JwtBearers;

/// <summary>
/// Fluent configuration for JWT Bearer authentication on the JobMaster HTTP API.
/// Use this selector to register the token validation strategy — either a built-in provider
/// backed by standard <see cref="TokenValidationParameters"/> or a fully custom implementation.
/// All methods return void; chain them in sequence during DI setup.
/// </summary>
public interface IJwtBearerAuthConfigSelector
{
    /// <summary>
    /// Registers a custom JWT Bearer authentication provider.
    /// The provider is resolved from the DI container, giving you full control over
    /// token extraction, validation, and claims mapping.
    /// </summary>
    /// <typeparam name="T">A class that implements <see cref="IJobMasterJwtBearerAuthProvider"/> and is registered in DI.</typeparam>
    void RegisterJwtBearerAuthProvider<T>() where T : class, IJobMasterJwtBearerAuthProvider;

    /// <summary>
    /// Registers the built-in JWT Bearer provider using standard ASP.NET Core token validation.
    /// Supply the <paramref name="parameters"/> to configure issuer, audience, signing key, and other validation rules.
    /// </summary>
    /// <param name="parameters">The <see cref="TokenValidationParameters"/> used to validate incoming JWT tokens.</param>
    void RegisterDefaultJwtBearerAuthProvider(TokenValidationParameters parameters);

    /// <summary>
    /// Overrides the HTTP header name from which the JWT token is read.
    /// Defaults to the standard <c>Authorization</c> header.
    /// </summary>
    /// <param name="headerName">The custom header name to use for token extraction.</param>
    void AuthorizationHeaderName(string headerName);

    /// <summary>
    /// Overrides the authentication scheme prefix expected before the token value in the header.
    /// Defaults to <c>Bearer</c> (e.g. <c>Authorization: Bearer &lt;token&gt;</c>).
    /// </summary>
    /// <param name="scheme">The scheme prefix string.</param>
    void Scheme(string scheme);
}
