namespace JobMaster.Api.AspNetCore.Auth.UsersAndPasswords;

/// <summary>
/// Fluent configuration for username/password authentication on the JobMaster HTTP API.
/// Use this selector to register fixed user identities or a custom credential provider.
/// All methods return void; call them in sequence during DI setup.
/// </summary>
public interface IApiUserPwdAuthConfigSelector
{
    /// <summary>
    /// Add a fixed user identity. By Default the Password is hashed using SHA256.
    /// </summary>
    /// <param name="userName"></param>
    /// <param name="planPwd"></param>
    /// <param name="claims"></param>
    IApiUserPwdAuthConfigSelector AddUserPwd(string userName, string planPwd, IDictionary<string, string>? claims = null);

    /// <summary>
    /// Register a custom User Password authentication provider.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    IApiUserPwdAuthConfigSelector RegisterUserPwdAuthProvider<T>() where T : class, IJobMasterUserPwdAuthProvider;
    
    /// <summary>
    /// Configure the User Name header name.
    /// </summary>
    /// <param name="headerName">The header name to use for User Name authentication. Default is "X-User-Name".</param>
    IApiUserPwdAuthConfigSelector UserNameHeaderName(string headerName);
    
    /// <summary>
    /// Configure the Password header name.
    /// </summary>
    /// <param name="headerName">The header name to use for Password authentication. Default is "X-Password".</param>
    IApiUserPwdAuthConfigSelector PwdHeaderName(string headerName);
}