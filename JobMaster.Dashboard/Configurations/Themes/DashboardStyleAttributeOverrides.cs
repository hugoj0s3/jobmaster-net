namespace JobMaster.Dashboard.Configurations.Themes;

public class DashboardStyleAttributeOverrides
{
    /// <summary>
    /// The sans-serif font family stack applied to <c>--font-sans</c>.
    /// Use standard CSS font-family values, e.g. <c>["Geist", "ui-sans-serif"]</c>.
    /// Pair with <see cref="FontUrlSans"/> if the font needs to be loaded from an external stylesheet.
    /// </summary>
    public string[]? FontFamilySans { get; set; }

    /// <summary>
    /// URL of a CSS stylesheet that loads the font declared in <see cref="FontFamilySans"/>.
    /// The dashboard injects a <c>&lt;link rel="stylesheet"&gt;</c> tag at runtime with this URL.
    /// Accepts Google Fonts, Bunny Fonts, or any self-hosted stylesheet URL.
    /// </summary>
    public string? FontUrlSans { get; set; }

    /// <summary>
    /// The monospace font family stack applied to <c>--font-mono</c>.
    /// Use standard CSS font-family values, e.g. <c>["Fira Code", "monospace"]</c>.
    /// Pair with <see cref="FontUrlMono"/> if the font needs to be loaded from an external stylesheet.
    /// </summary>
    public string[]? FontFamilyMono { get; set; }

    /// <summary>
    /// URL of a CSS stylesheet that loads the font declared in <see cref="FontFamilyMono"/>.
    /// The dashboard injects a <c>&lt;link rel="stylesheet"&gt;</c> tag at runtime with this URL.
    /// Accepts Google Fonts, Bunny Fonts, or any self-hosted stylesheet URL.
    /// </summary>
    public string? FontUrlMono { get; set; }

    /// <summary>
    /// Border radius for box-like components: Cards, Modals, Alerts (<c>--radius-box</c>).
    /// </summary>
    public string? BorderRadiusBox { get; set; }

    /// <summary>
    /// Border radius for selector-like components: Buttons, Dropdowns, Tabs (<c>--radius-selector</c>).
    /// </summary>
    public string? BorderRadiusBtn { get; set; }

    /// <summary>
    /// Border radius for field-like components: Inputs, Textareas, Selects (<c>--radius-field</c>).
    /// </summary>
    public string? BorderRadiusBadge { get; set; }
}
