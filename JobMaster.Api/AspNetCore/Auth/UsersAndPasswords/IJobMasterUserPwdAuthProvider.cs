namespace JobMaster.Api.AspNetCore.Auth.UsersAndPasswords;

public interface IJobMasterUserPwdAuthProvider
{
    Task<JobMasterUserPwdIdentity?> GetUserPwdIdentityAsync(string userName);
    bool Verify(string planPassword, string hashedPassword); 
}