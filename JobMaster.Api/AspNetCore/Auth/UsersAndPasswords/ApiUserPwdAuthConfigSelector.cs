namespace JobMaster.Api.AspNetCore.Auth.UsersAndPasswords;

internal class ApiUserPwdAuthConfigSelector : IApiUserPwdAuthConfigSelector
{
    private readonly JobMasterApiOptions jobMasterOptions;
    
    public ApiUserPwdAuthConfigSelector(JobMasterApiOptions jobMasterOptions)
    {
        jobMasterOptions.RequireAuthentication = true;
        this.jobMasterOptions = jobMasterOptions;
    }
    
    public void AddUserPwd(string userName, string planPwd, IDictionary<string, string>? claims = null)
    {
        this.jobMasterOptions.EnsureUserPwdOptionsIsEnabled();
        this.jobMasterOptions.UserPwdOptions!.FixedIdentityPlanPwdList.Add(
            new JobMasterUserPwdIdentity
            {
                UserName = userName, 
                HashedPassword = JobMasterPasswordHasher.Hash(planPwd),
                Claims = claims
            });
    }

    public void RegisterUserPwdAuthProvider<T>() where T : class, IJobMasterUserPwdAuthProvider
    {
        this.jobMasterOptions.EnsureUserPwdOptionsIsEnabled();
        this.jobMasterOptions.UserPwdOptions!.UserPwdAuthProviderType = typeof(T);
    }

    public void UserNameHeaderName(string headerName)
    {
        this.jobMasterOptions.EnsureUserPwdOptionsIsEnabled();
        this.jobMasterOptions.UserPwdOptions!.UserHeaderName = headerName;
    }

    public void PwdHeaderName(string headerName)
    {
        this.jobMasterOptions.EnsureUserPwdOptionsIsEnabled();
        this.jobMasterOptions.UserPwdOptions!.PwdHeaderName = headerName;
    }
}