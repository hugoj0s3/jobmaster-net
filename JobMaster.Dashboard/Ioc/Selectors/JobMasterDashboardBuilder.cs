using JobMaster.Dashboard.Configurations;
using JobMaster.Dashboard.Configurations.Auth;
using JobMaster.Dashboard.Configurations.Public;
using JobMaster.Dashboard.Configurations.Themes;
using JobMaster.Dashboard.Ioc.Selectors.Auth;
using JobMaster.Dashboard.Ioc.Selectors.AuthRetention;
using JobMaster.Dashboard.Ioc.Selectors.Themes;

namespace JobMaster.Dashboard.Ioc.Selectors;

internal class JobMasterDashboardBuilder : IJobMasterDashboardBuilder
{
    private readonly DashboardOptions options;

    public JobMasterDashboardBuilder(DashboardOptions options)
    {
        this.options = options;
    }

    public IJobMasterDashboardBuilder UseBasePath(string basePath)
    {
        var trimmed = basePath.Trim('/');
        if (trimmed.Contains('/'))
            throw new ArgumentException(
                $"BasePath must be a single path segment (e.g. \"/jm-dashboard\"), not a multi-segment path. Got: \"{basePath}\"",
                nameof(basePath));

        options.BasePath = string.IsNullOrEmpty(trimmed) ? string.Empty : "/" + trimmed;
        return this;
    }

    public IJobMasterDashboardBuilder UseApiUrl(string apiUrl)
    {
        options.ApiUrl = apiUrl;
        return this;
    }

    public IJobMasterDashboardBuilder ConfigCluster(string id, string? environmentName = null, bool disabled = false)
    {
        var existing = options.Clusters.FirstOrDefault(c => c.Id == id);
        if (existing is not null)
        {
            if (environmentName is not null) existing.EnvironmentName = environmentName;
            existing.Disabled = disabled;
        }
        else
        {
            options.Clusters.Add(new DashboardClusterConfig
            {
                Id = id,
                EnvironmentName = environmentName,
                Disabled = disabled
            });
        }
        return this;
    }

    public IJobMasterDashboardBuilder DisableCluster(string id) => ConfigCluster(id, disabled: true);

    public IJobMasterDashboardThemeSelector AddTheme(DashboardBuiltInTheme theme, string? displayName = null)
    {
        var config = new DashboardThemeItemConfig
        {
            BaseTheme = theme,
            DisplayName = displayName ?? theme.ToString()
        };
        options.ThemeConfigs.Themes.Add(config);
        return new DashboardThemeBuilder(options, config);
    }

    public IJobMasterDashboardPrimaryThemeSelector AddPrimaryTheme(DashboardBuiltInTheme theme, string? displayName = null)
    {
        var config = new DashboardThemeItemConfig
        {
            BaseTheme = theme,
            DisplayName = displayName ?? theme.ToString(),
            IsPrimaryTheme = true
        };
        options.ThemeConfigs.Themes.Add(config);
        options.ThemeConfigs.PrimaryThemeId = DashboardPublicConfigConvertUtil.GenerateThemeId(config.DisplayName);
        return new DashboardThemeBuilder(options, config);
    }

    public IJobMasterDashboardApiKeyAuthSelector ConfigApiKeyAuth()
    {
        var existing = options.Auth.Providers.OfType<ApiKeyAuthProviderConfig>().FirstOrDefault();
        if (existing is not null)
            return new JobMasterDashboardApiKeyAuthSelector(existing);

        options.Auth.Enabled = true;
        var config = new ApiKeyAuthProviderConfig();
        options.Auth.Providers.Add(config);
        return new JobMasterDashboardApiKeyAuthSelector(config);
    }

    public IJobMasterDashboardUserPasswordAuthSelector ConfigUserPasswordAuth()
    {
        var existing = options.Auth.Providers.OfType<UserPasswordAuthProviderConfig>().FirstOrDefault();
        if (existing is not null)
            return new JobMasterDashboardUserPasswordAuthSelector(existing);

        options.Auth.Enabled = true;
        var config = new UserPasswordAuthProviderConfig();
        options.Auth.Providers.Add(config);
        return new JobMasterDashboardUserPasswordAuthSelector(config);
    }

    public IJobMasterDashboardSimpleJwtAuthSelector ConfigSimpleJwtAuth()
    {
        var existing = options.Auth.Providers.OfType<SimpleJwtAuthProviderConfig>().FirstOrDefault();
        if (existing is not null)
            return new JobMasterDashboardSimpleJwtAuthSelector(existing);

        options.Auth.Enabled = true;
        var config = new SimpleJwtAuthProviderConfig();
        options.Auth.Providers.Add(config);
        return new JobMasterDashboardSimpleJwtAuthSelector(config);
    }

    public IJobMasterDashboardJwtFormAuthSelector ConfigJwtFormAuth(string tokenUrl)
    {
        var existing = options.Auth.Providers.OfType<JwtFormAuthProviderConfig>().FirstOrDefault();
        if (existing is not null)
        {
            existing.TokenUrl = tokenUrl;
            return new JobMasterDashboardJwtFormAuthSelector(existing);
        }

        options.Auth.Enabled = true;
        var config = new JwtFormAuthProviderConfig { TokenUrl = tokenUrl };
        options.Auth.Providers.Add(config);
        return new JobMasterDashboardJwtFormAuthSelector(config);
    }

    public IJobMasterDashboardBuilder DisableAuth(DashboardAuthProviderId providerId)
    {
        var existing = options.Auth.Providers.FirstOrDefault(p => p.ProviderId == providerId);
        if (existing is not null)
        {
            existing.Disabled = true;
            return this;
        }

        DashboardAuthProviderConfig placeholder = providerId switch
        {
            DashboardAuthProviderId.ApiKey => new ApiKeyAuthProviderConfig { Disabled = true },
            DashboardAuthProviderId.UserPassword => new UserPasswordAuthProviderConfig { Disabled = true },
            DashboardAuthProviderId.SimpleJwt => new SimpleJwtAuthProviderConfig { Disabled = true },
            DashboardAuthProviderId.JwtForm => new JwtFormAuthProviderConfig { Disabled = true },
            _ => throw new ArgumentOutOfRangeException(nameof(providerId))
        };
        options.Auth.Providers.Add(placeholder);
        return this;
    }

    public IJobMasterDashboardBuilder FromOpenApiJson(string urlOrPath = "")
    {
        options.OpenApiUrl = urlOrPath;
        return this;
    }

    public IJobMasterDashboardAuthRetentionSelector ConfigureAuthRetention()
    {
        return new JobMasterDashboardAuthRetentionSelector(options.AuthRetention);
    }
}
