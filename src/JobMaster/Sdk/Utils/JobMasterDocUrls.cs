namespace JobMaster.Sdk.Utils;

internal static class JobMasterDocUrls
{
    private const string BaseUrl = "https://docs.jobmaster.hugoj0s3.dev/docs/";

    // Page paths kept here so a doc move/rename is a one-line fix instead of a string hunt.
    public static class Pages
    {
        public const string NatsProvider = "configuration/providers/nats";
    }

    public static string Page(string pagePath, string? reference = null)
    {
        var url = BaseUrl + pagePath.Trim('/');
        return string.IsNullOrWhiteSpace(reference) ? url : $"{url}#{reference}";
    }
}