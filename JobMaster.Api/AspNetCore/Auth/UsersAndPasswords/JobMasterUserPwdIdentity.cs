namespace JobMaster.Api.AspNetCore.Auth.UsersAndPasswords;

public sealed class JobMasterUserPwdIdentity
{
    public string UserName { get; set; } = string.Empty;
    public string HashedPassword { get; set; } = string.Empty;
    public IDictionary<string, string>? Claims = null;
}