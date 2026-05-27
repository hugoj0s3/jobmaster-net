using JobMaster.Dashboard.Configurations.Auth;

namespace JobMaster.Dashboard.Ioc.Selectors.Auth;

internal class JobMasterDashboardJwtFormAuthSelector : IJobMasterDashboardJwtFormAuthSelector
{
    private readonly JwtFormAuthProviderConfig config;

    public JobMasterDashboardJwtFormAuthSelector(JwtFormAuthProviderConfig config)
    {
        this.config = config;
    }

    public IJobMasterDashboardJwtFormAuthSelector WithDisplayName(string displayName)
    {
        this.config.DisplayName = displayName;
        return this;
    }

    public IJobMasterDashboardJwtFormAuthSelector WithTokenUrl(string tokenUrl)
    {
        this.config.TokenUrl = tokenUrl;
        return this;
    }

    public IJobMasterDashboardJwtFormAuthSelector WithHeaderName(string headerName)
    {
        this.config.HeaderName = headerName;
        return this;
    }

    public IJobMasterDashboardJwtFormAuthSelector WithScheme(string scheme)
    {
        this.config.Scheme = scheme;
        return this;
    }

    public IJobMasterDashboardJwtFormAuthSelector AddField(
        string id,
        string label,
        DashboardJwtFormFieldType type = DashboardJwtFormFieldType.Text,
        bool isRequired = true,
        string? defaultValue = null,
        bool isDisabled = false)
    {
        var fieldConfig = new JwtFormFieldConfig 
        { 
            Id = id,
            Label = label,
            Type = type,
            IsRequired = isRequired,
            DefaultValue = defaultValue,
            Disabled = isDisabled
        };
        this.config.Fields.Add(fieldConfig);
        return this;
    }
}
