namespace JobMaster.Api.AspNetCore.Auth.UsersAndPasswords;

internal sealed class JobMasterUserPwdOptions
{
    public string UserHeaderName { get; set; } = "X-User-Name";
    public string PwdHeaderName { get; set; } = "X-Password";                                                                              
    
    internal Type? UserPwdAuthProviderType { get; set; } 
    
    internal IList<JobMasterUserPwdIdentity> FixedIdentityPlanPwdList { get; set; } = new List<JobMasterUserPwdIdentity>();
}