using JobMaster.Dashboard.Configurations;
using JobMaster.Dashboard.Configurations.Auth;
using JobMaster.Dashboard.Configurations.Public;
using JobMaster.Dashboard.Configurations.Themes;
using JobMaster.Dashboard.Ioc.Selectors;
using JobMaster.Dashboard.Ioc.Selectors.Auth;
using JobMaster.Dashboard.Ioc.Selectors.Credentials;
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
        options.BasePath = basePath;
        return this;
    }

    public IJobMasterDashboardBuilder UseApiUrl(string apiUrl)
    {
        options.ApiUrl = apiUrl;
        return this;
    }

    public IJobMasterDashboardBuilder AddCluster(string id, string environmentName)
    {
        options.Clusters.Add(new DashboardClusterConfig()
        {
            Id = id,
            EnvironmentName = environmentName,
        });
        return this;
    }

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

    public IJobMasterDashboardApiKeyAuthSelector AddApiKeyAuth()
    {
        options.Auth.Enabled = true;
        var config = new ApiKeyAuthProviderConfig();
        options.Auth.Providers.Add(config);
        return new JobMasterDashboardApiKeyAuthSelector(config);
    }

    public IJobMasterDashboardUserPasswordAuthSelector AddUserPasswordAuth()
    {
        options.Auth.Enabled = true;
        var config = new UserPasswordAuthProviderConfig();
        options.Auth.Providers.Add(config);
        return new JobMasterDashboardUserPasswordAuthSelector(config);
    }

    public IJobMasterDashboardSimpleJwtAuthSelector AddSimpleJwtAuth()
    {
        options.Auth.Enabled = true;
        var config = new SimpleJwtAuthProviderConfig();
        options.Auth.Providers.Add(config);
        return new JobMasterDashboardSimpleJwtAuthSelector(config);
    }

    public IJobMasterDashboardJwtFormAuthSelector AddJwtFormAuth(string tokenUrl)
    {
        options.Auth.Enabled = true;
        var config = new JwtFormAuthProviderConfig { TokenUrl = tokenUrl };
        options.Auth.Providers.Add(config);
        return new JobMasterDashboardJwtFormAuthSelector(config);
    }

    public IJobMasterDashboardBuilder FromOpenApi()
    {
        options.OpenApiUrl = string.Empty;
        return this;
    }

    public IJobMasterDashboardBuilder FromOpenApi(string urlOrPath)
    {
        options.OpenApiUrl = urlOrPath;
        return this;
    }

    public IJobMasterDashboardCredentialsPersistenceSelector ConfigureCredentialsPersistence()
    {
        return new JobMasterDashboardCredentialsPersistenceSelector(options.CredentialsPersistence);
    }
}