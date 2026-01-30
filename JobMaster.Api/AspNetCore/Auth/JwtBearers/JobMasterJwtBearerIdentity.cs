namespace JobMaster.Api.AspNetCore.Auth.JwtBearers;

public sealed class JobMasterJwtBearerIdentity
{
    public string Subject { get; set; } = string.Empty;

    public IDictionary<string, string> Claims { get; set; } = new Dictionary<string, string>();
}
