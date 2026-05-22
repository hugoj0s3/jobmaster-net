namespace JobMaster.Api.AspNetCore.Auth.UsersAndPasswords;

/// <summary>
/// Resolves and verifies user credentials for username/password authentication.
/// Implement this interface to provide custom user storage and password verification.
/// Register via <see cref="IApiUserPwdAuthConfigSelector.RegisterUserPwdAuthProvider{T}"/>.
/// </summary>
public interface IJobMasterUserPwdAuthProvider
{
    /// <summary>
    /// Looks up the identity for <paramref name="userName"/>.
    /// Returns <c>null</c> if the user is not found.
    /// </summary>
    Task<JobMasterUserPwdIdentity?> GetUserPwdIdentityAsync(string userName);

    /// <summary>
    /// Verifies that <paramref name="planPassword"/> matches <paramref name="hashedPassword"/>.
    /// </summary>
    bool Verify(string planPassword, string hashedPassword);
}