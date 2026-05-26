namespace JobMaster.Api.AspNetCore.Auth.UsersAndPasswords;

/// <summary>Represents the stored identity for a username/password user.</summary>
public sealed class JobMasterUserPwdIdentity
{
    /// <summary>The username.</summary>
    public string UserName { get; set; } = string.Empty;
    /// <summary>The hashed password stored for verification.</summary>
    public string HashedPassword { get; set; } = string.Empty;
    /// <summary>Optional claims attached to this identity.</summary>
    public IDictionary<string, string>? Claims = null;
}