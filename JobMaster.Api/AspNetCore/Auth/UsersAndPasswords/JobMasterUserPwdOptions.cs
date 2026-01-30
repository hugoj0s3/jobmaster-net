namespace JobMaster.Api.AspNetCore.Auth.UsersAndPasswords;

internal sealed class JobMasterUserPwdOptions
{
    public string UserHeaderName { get; set; } = "user";
    public string PwdHeaderName { get; set; } = "pwd";                                                                              
    
    internal Type? UserPwdAuthProviderType { get; set; } 
    
    internal IList<JobMasterUserPwdIdentity> FixedIdentityPlanPwdList { get; set; } = new List<JobMasterUserPwdIdentity>();
}