using Microsoft.AspNetCore.Http;

namespace JobMaster.Api.AspNetCore.Auth;

internal static class AnonymousUserConstants
{
    public static readonly JobMasterApiIdentity Anonymous = new(
        IsAuthenticated: false,
        Subject: "Anonymous",
        AuthenticationType: null
    );
}
