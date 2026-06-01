using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;
using JobMaster.Dashboard.Configurations.Auth;
using JobMaster.Dashboard.Configurations.Themes;

namespace JobMaster.Dashboard.Configurations.Public;

internal static class DashboardPublicConfigConvertUtil
{
    public static DashboardPublicConfig ToPublicConfig(DashboardOptions options)
    {
        return new DashboardPublicConfig
        {
            ApiBaseUrl = options.ApiUrl,
            BasePath = options.BasePath,
            AuthRetentionMode = options.AuthRetention.AuthRetentionType switch
            {
                DashboardAuthRetentionType.ClientSideSessionStorage => "client",
                DashboardAuthRetentionType.ServerSideInMemory       => "server",
                DashboardAuthRetentionType.ServerSideDistributed    => "server",
                _                                                   => "none",
            },
            Auth = ToPublicAuth(options.Auth),
            Clusters = options.Clusters.Where(c => !c.Disabled).Select(ToPublicCluster).ToList(),
            ThemeConfigs = ToPublicTheme(options.ThemeConfigs)
        };
    }

    private static DashboardPublicClusterConfig ToPublicCluster(DashboardClusterConfig cluster)
    {
        return new DashboardPublicClusterConfig
        {
            Id = cluster.Id,
            EnvironmentName = cluster.EnvironmentName,
            ThemeId = cluster.ThemeId
        };
    }

    private static PublicAuthConfig ToPublicAuth(DashboardAuthConfig auth)
    {
        return new PublicAuthConfig
        {
            Enabled = auth.Enabled,
            Providers = auth.Providers.Where(p => !p.Disabled).Select(ToPublicAuthProvider).ToList()
        };
    }

    private static PublicAuthProviderConfig ToPublicAuthProvider(DashboardAuthProviderConfig provider)
    {
        var config = new PublicAuthProviderConfig
        {
            Type = ToProviderTypeString(provider.ProviderId),
            DisplayName = provider.DisplayName
        };

        switch (provider)
        {
            case ApiKeyAuthProviderConfig apiKey:
                config.HeaderName = apiKey.HeaderName;
                break;
            case UserPasswordAuthProviderConfig userPwd:
                config.UserHeaderName = userPwd.UserHeaderName;
                config.PasswordHeaderName = userPwd.PasswordHeaderName;
                break;
            case SimpleJwtAuthProviderConfig simpleJwt:
                config.HeaderName = simpleJwt.HeaderName;
                config.Scheme = simpleJwt.Scheme;
                break;
            case JwtFormAuthProviderConfig jwtForm:
                config.TokenUrl = jwtForm.TokenUrl;
                config.HeaderName = jwtForm.HeaderName;
                config.Scheme = jwtForm.Scheme;
                config.Fields = jwtForm.Fields.Select(ToPublicJwtFormField).ToList();
                break;
        }

        return config;
    }

    private static PublicJwtFormFieldConfig ToPublicJwtFormField(JwtFormFieldConfig field)
    {
        return new PublicJwtFormFieldConfig
        {
            Id = field.Id,
            Label = field.Label,
            Type = ToFieldTypeString(field.Type),
            IsRequired = field.IsRequired,
            DefaultValue = field.DefaultValue,
            Disabled = field.Disabled
        };
    }

    private static PublicThemeConfig ToPublicTheme(DashboardThemesConfig theme)
    {
        return new PublicThemeConfig
        {
            PrimaryThemeId = theme.PrimaryThemeId ?? "jobmaster-light",
            Themes = theme.Themes.Count > 0
                ? theme.Themes.Select(ToPublicThemeItem).ToList()
                : DefaultThemes()
        };
    }

    private static IList<PublicThemeItemConfig> DefaultThemes() =>
    [
        new() { Id = "jobmaster-light", DisplayName = "JobMaster Light", BaseTheme = "jobmaster-light", IsPrimaryTheme = true },
        new() { Id = "jobmaster-dark",  DisplayName = "JobMaster Dark",  BaseTheme = "jobmaster-dark",  IsPrimaryTheme = false }
    ];

    private static PublicThemeItemConfig ToPublicThemeItem(DashboardThemeItemConfig theme)
    {
        return new PublicThemeItemConfig
        {
            Id = GenerateThemeId(theme.DisplayName),
            DisplayName = theme.DisplayName,
            BaseTheme = ToDaisyThemeName(theme.BaseTheme),
            IsPrimaryTheme = theme.IsPrimaryTheme,
            ColorOverrides = HasColorOverrides(theme.ColorOverrides) ? theme.ColorOverrides : null,
            StyleOverrides = theme.IsPrimaryTheme ? theme.StyleOverrides : null
        };
    }

    internal static string GenerateThemeId(string displayName)
    {
        var normalized = displayName.Normalize(NormalizationForm.FormD);
        var sb = new StringBuilder();
        foreach (var c in normalized)
        {
            if (CharUnicodeInfo.GetUnicodeCategory(c) == UnicodeCategory.NonSpacingMark) continue;
            sb.Append(char.IsLetterOrDigit(c) ? char.ToLowerInvariant(c) : '-');
        }
        return Regex.Replace(sb.ToString(), "-+", "-").Trim('-');
    }

    private static bool HasColorOverrides(DashboardPublicColorOverrides o) =>
        o.Logo != null || o.LogoContent != null ||
        o.Primary != null || o.PrimaryContent != null ||
        o.Secondary != null || o.SecondaryContent != null ||
        o.Accent != null || o.AccentContent != null ||
        o.Base100 != null || o.Base200 != null || o.Base300 != null || o.BaseContent != null ||
        o.Neutral != null || o.NeutralContent != null ||
        o.Info != null || o.InfoContent != null ||
        o.Success != null || o.SuccessContent != null ||
        o.Warning != null || o.WarningContent != null ||
        o.Error != null || o.ErrorContent != null;

    private static string ToDaisyThemeName(DashboardBuiltInTheme theme) => theme switch
    {
        DashboardBuiltInTheme.JobMasterLight => "jobmaster-light",
        DashboardBuiltInTheme.JobMasterDark  => "jobmaster-dark",
        _                                  => theme.ToString().ToLowerInvariant()
    };

    private static string ToProviderTypeString(DashboardAuthProviderId id) => id switch
    {
        DashboardAuthProviderId.ApiKey       => "API_KEY",
        DashboardAuthProviderId.UserPassword => "USER_PASSWORD",
        DashboardAuthProviderId.SimpleJwt    => "JWT_SIMPLE",
        DashboardAuthProviderId.JwtForm      => "JWT_CUSTOM_FORM",
        _                                    => id.ToString().ToUpperInvariant()
    };

    private static string ToFieldTypeString(DashboardJwtFormFieldType type) => type switch
    {
        DashboardJwtFormFieldType.Text     => "text",
        DashboardJwtFormFieldType.Password => "password",
        DashboardJwtFormFieldType.Email    => "email",
        DashboardJwtFormFieldType.Number   => "number",
        DashboardJwtFormFieldType.Hidden   => "hidden",
        DashboardJwtFormFieldType.TextArea => "textarea",
        DashboardJwtFormFieldType.Checkbox => "checkbox",
        _                                  => type.ToString().ToLowerInvariant()
    };
}
