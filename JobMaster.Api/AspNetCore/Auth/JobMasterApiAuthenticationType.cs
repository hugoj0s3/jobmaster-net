namespace JobMaster.Api.AspNetCore.Auth;

public enum JobMasterApiAuthenticationType
{
    UserPwd,
    ApiKey,
    JwtBearer,
    Customized,
}