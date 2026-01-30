namespace JobMaster.Api.AspNetCore.Auth.UsersAndPasswords;

public interface IApiUserPwdAuthConfigSelector
{
    void AddUserPwd(string userName, string planPwd, IDictionary<string, string>? claims = null);

    void RegisterUserPwdAuthProvider<T>() where T : class, IJobMasterUserPwdAuthProvider;
    
    void UserNameHeaderName(string headerName);
    
    void PwdHeaderName(string headerName);
}