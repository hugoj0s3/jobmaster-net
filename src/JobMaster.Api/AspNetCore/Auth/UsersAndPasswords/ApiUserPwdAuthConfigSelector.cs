namespace JobMaster.Api.AspNetCore.Auth.UsersAndPasswords;

internal class ApiUserPwdAuthConfigSelector : IApiUserPwdAuthConfigSelector
{
    private readonly JobMasterApiOptions jobMasterOptions;
    
    public ApiUserPwdAuthConfigSelector(JobMasterApiOptions jobMasterOptions)
    {
        jobMasterOptions.RequireAuthentication = true;
        this.jobMasterOptions = jobMasterOptions;
    }
    
    public IApiUserPwdAuthConfigSelector AddUserPwd(string userName, string planPwd, IDictionary<string, string>? claims = null)
    {
        this.jobMasterOptions.EnsureUserPwdOptionsIsEnabled();
        this.jobMasterOptions.UserPwdOptions!.FixedIdentityPlanPwdList.Add(
            new JobMasterUserPwdIdentity
            {
                UserName = userName, 
                HashedPassword = JobMasterPasswordHasher.Hash(planPwd),
                Claims = claims
            });
        return this;
    }

    public IApiUserPwdAuthConfigSelector RegisterUserPwdAuthProvider<T>() where T : class, IJobMasterUserPwdAuthProvider
    {
        this.jobMasterOptions.EnsureUserPwdOptionsIsEnabled();
        this.jobMasterOptions.UserPwdOptions!.UserPwdAuthProviderType = typeof(T);
        return this;
    }

    public IApiUserPwdAuthConfigSelector UserNameHeaderName(string headerName)
    {
        this.jobMasterOptions.EnsureUserPwdOptionsIsEnabled();
        this.jobMasterOptions.UserPwdOptions!.UserHeaderName = headerName;
        return this;
    }

    public IApiUserPwdAuthConfigSelector PwdHeaderName(string headerName)
    {
        this.jobMasterOptions.EnsureUserPwdOptionsIsEnabled();
        this.jobMasterOptions.UserPwdOptions!.PwdHeaderName = headerName;
        return this;
    }
}