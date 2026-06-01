using JobMaster.Dashboard.Configurations;
using JobMaster.Dashboard.Configurations.Public;
using JobMaster.Dashboard.Configurations.Themes;

namespace JobMaster.Dashboard.Ioc.Selectors.Themes;

internal sealed class DashboardThemeBuilder : IJobMasterDashboardThemeSelector, IJobMasterDashboardPrimaryThemeSelector
{
    private readonly DashboardOptions options;
    private readonly DashboardThemeItemConfig theme;
    private readonly string themeId;

    internal DashboardThemeBuilder(DashboardOptions options, DashboardThemeItemConfig theme)
    {
        this.options = options;
        this.theme = theme;
        themeId = DashboardPublicConfigConvertUtil.GenerateThemeId(theme.DisplayName);
    }

    // ── IJobMasterDashboardThemeBaseSelector ─────────────────────────────────

    public IJobMasterDashboardThemeSelector DefaultForClusterId(string clusterId)
    {
        var cluster = options.Clusters.FirstOrDefault(c => c.Id == clusterId);
        if (cluster is not null) cluster.ThemeId = themeId;
        return this;
    }

    public IJobMasterDashboardThemeSelector DefaultForClusterIds(params string[] clusterIds)
    {
        foreach (var id in clusterIds) DefaultForClusterId(id);
        return this;
    }

    public IJobMasterDashboardPrimaryThemeSelector MakePrimary()
    {
        theme.IsPrimaryTheme = true;
        options.ThemeConfigs.PrimaryThemeId = themeId;
        return this;
    }

    // ── IJobMasterDashboardPrimaryThemeSelector ───────────────────────────────

    public IJobMasterDashboardPrimaryThemeSelector SetBorderRadii(string? box = null, string? selector = null, string? field = null)
    {
        if (box is not null) theme.StyleOverrides.BorderRadiusBox = box;
        if (selector is not null) theme.StyleOverrides.BorderRadiusBtn = selector;
        if (field is not null) theme.StyleOverrides.BorderRadiusBadge = field;
        return this;
    }

    // ── IJobMasterDashboardFontFamilySelector<IJobMasterDashboardPrimaryThemeSelector> ──

    IJobMasterDashboardPrimaryThemeSelector IJobMasterDashboardFontFamilySelector<IJobMasterDashboardPrimaryThemeSelector>.SetFontSans(string[] fontFamilies, string fontUrl) => ApplyFontSans(fontFamilies, fontUrl);
    IJobMasterDashboardPrimaryThemeSelector IJobMasterDashboardFontFamilySelector<IJobMasterDashboardPrimaryThemeSelector>.SetFontMono(string[] fontFamilies, string fontUrl) => ApplyFontMono(fontFamilies, fontUrl);

    // ── IJobMasterDashboardThemeColorSelector<IJobMasterDashboardThemeSelector> ──

    IJobMasterDashboardThemeSelector IJobMasterDashboardThemeColorSelector<IJobMasterDashboardThemeSelector>.Logo(string color, string content) => ApplyLogo(color, content);
    IJobMasterDashboardThemeSelector IJobMasterDashboardThemeColorSelector<IJobMasterDashboardThemeSelector>.Primary(string color, string content) => ApplyPrimary(color, content);
    IJobMasterDashboardThemeSelector IJobMasterDashboardThemeColorSelector<IJobMasterDashboardThemeSelector>.Secondary(string color, string content) => ApplySecondary(color, content);
    IJobMasterDashboardThemeSelector IJobMasterDashboardThemeColorSelector<IJobMasterDashboardThemeSelector>.Accent(string color, string content) => ApplyAccent(color, content);
    IJobMasterDashboardThemeSelector IJobMasterDashboardThemeColorSelector<IJobMasterDashboardThemeSelector>.Neutral(string color, string content) => ApplyNeutral(color, content);
    IJobMasterDashboardThemeSelector IJobMasterDashboardThemeColorSelector<IJobMasterDashboardThemeSelector>.BaseColors(string base100, string base200, string base300, string baseContent) => ApplyBaseColors(base100, base200, base300, baseContent);
    IJobMasterDashboardThemeSelector IJobMasterDashboardThemeColorSelector<IJobMasterDashboardThemeSelector>.Info(string color, string content) => ApplyInfo(color, content);
    IJobMasterDashboardThemeSelector IJobMasterDashboardThemeColorSelector<IJobMasterDashboardThemeSelector>.Success(string color, string content) => ApplySuccess(color, content);
    IJobMasterDashboardThemeSelector IJobMasterDashboardThemeColorSelector<IJobMasterDashboardThemeSelector>.Warning(string color, string content) => ApplyWarning(color, content);
    IJobMasterDashboardThemeSelector IJobMasterDashboardThemeColorSelector<IJobMasterDashboardThemeSelector>.Error(string color, string content) => ApplyError(color, content);

    // ── IJobMasterDashboardThemeColorSelector<IJobMasterDashboardPrimaryThemeSelector> ──

    IJobMasterDashboardPrimaryThemeSelector IJobMasterDashboardThemeColorSelector<IJobMasterDashboardPrimaryThemeSelector>.Logo(string color, string content) => ApplyLogo(color, content);
    IJobMasterDashboardPrimaryThemeSelector IJobMasterDashboardThemeColorSelector<IJobMasterDashboardPrimaryThemeSelector>.Primary(string color, string content) => ApplyPrimary(color, content);
    IJobMasterDashboardPrimaryThemeSelector IJobMasterDashboardThemeColorSelector<IJobMasterDashboardPrimaryThemeSelector>.Secondary(string color, string content) => ApplySecondary(color, content);
    IJobMasterDashboardPrimaryThemeSelector IJobMasterDashboardThemeColorSelector<IJobMasterDashboardPrimaryThemeSelector>.Accent(string color, string content) => ApplyAccent(color, content);
    IJobMasterDashboardPrimaryThemeSelector IJobMasterDashboardThemeColorSelector<IJobMasterDashboardPrimaryThemeSelector>.Neutral(string color, string content) => ApplyNeutral(color, content);
    IJobMasterDashboardPrimaryThemeSelector IJobMasterDashboardThemeColorSelector<IJobMasterDashboardPrimaryThemeSelector>.BaseColors(string base100, string base200, string base300, string baseContent) => ApplyBaseColors(base100, base200, base300, baseContent);
    IJobMasterDashboardPrimaryThemeSelector IJobMasterDashboardThemeColorSelector<IJobMasterDashboardPrimaryThemeSelector>.Info(string color, string content) => ApplyInfo(color, content);
    IJobMasterDashboardPrimaryThemeSelector IJobMasterDashboardThemeColorSelector<IJobMasterDashboardPrimaryThemeSelector>.Success(string color, string content) => ApplySuccess(color, content);
    IJobMasterDashboardPrimaryThemeSelector IJobMasterDashboardThemeColorSelector<IJobMasterDashboardPrimaryThemeSelector>.Warning(string color, string content) => ApplyWarning(color, content);
    IJobMasterDashboardPrimaryThemeSelector IJobMasterDashboardThemeColorSelector<IJobMasterDashboardPrimaryThemeSelector>.Error(string color, string content) => ApplyError(color, content);

    // ── Private helpers ───────────────────────────────────────────────────────

    private DashboardThemeBuilder ApplyLogo(string color, string content) { theme.ColorOverrides.Logo = color; theme.ColorOverrides.LogoContent = content; return this; }
    private DashboardThemeBuilder ApplyPrimary(string color, string content) { theme.ColorOverrides.Primary = color; theme.ColorOverrides.PrimaryContent = content; return this; }
    private DashboardThemeBuilder ApplySecondary(string color, string content) { theme.ColorOverrides.Secondary = color; theme.ColorOverrides.SecondaryContent = content; return this; }
    private DashboardThemeBuilder ApplyAccent(string color, string content) { theme.ColorOverrides.Accent = color; theme.ColorOverrides.AccentContent = content; return this; }
    private DashboardThemeBuilder ApplyNeutral(string color, string content) { theme.ColorOverrides.Neutral = color; theme.ColorOverrides.NeutralContent = content; return this; }
    private DashboardThemeBuilder ApplyInfo(string color, string content) { theme.ColorOverrides.Info = color; theme.ColorOverrides.InfoContent = content; return this; }
    private DashboardThemeBuilder ApplySuccess(string color, string content) { theme.ColorOverrides.Success = color; theme.ColorOverrides.SuccessContent = content; return this; }
    private DashboardThemeBuilder ApplyWarning(string color, string content) { theme.ColorOverrides.Warning = color; theme.ColorOverrides.WarningContent = content; return this; }
    private DashboardThemeBuilder ApplyError(string color, string content) { theme.ColorOverrides.Error = color; theme.ColorOverrides.ErrorContent = content; return this; }

    private DashboardThemeBuilder ApplyBaseColors(string base100, string base200, string base300, string baseContent)
    {
        theme.ColorOverrides.Base100 = base100;
        theme.ColorOverrides.Base200 = base200;
        theme.ColorOverrides.Base300 = base300;
        theme.ColorOverrides.BaseContent = baseContent;
        return this;
    }

    private DashboardThemeBuilder ApplyFontSans(string[] fontFamilies, string fontUrl) { theme.StyleOverrides.FontFamilySans = fontFamilies; theme.StyleOverrides.FontUrlSans = fontUrl; return this; }
    private DashboardThemeBuilder ApplyFontMono(string[] fontFamilies, string fontUrl) { theme.StyleOverrides.FontFamilyMono = fontFamilies; theme.StyleOverrides.FontUrlMono = fontUrl; return this; }
}
