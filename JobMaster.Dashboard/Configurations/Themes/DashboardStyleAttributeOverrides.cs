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
    /// Controls the border radius for large components like Cards, Modals, and Alerts (--rounded-box).
    /// </summary>
    public string? BorderRadiusBox { get; set; }

    /// <summary>
    /// Controls the border radius for medium components like Buttons, Inputs, Selects, and Menus (--rounded-btn).
    /// </summary>
    public string? BorderRadiusBtn { get; set; }

    /// <summary>
    /// Controls the border radius for small components like Badges and Toggles (--rounded-badge).
    /// </summary>
    public string? BorderRadiusBadge { get; set; }

    /// <summary>
    /// Controls the border radius specifically for Tabs (--tab-radius).
    /// </summary>
    public string? TabRadius { get; set; }

    /// <summary>
    /// Controls the border width of buttons (--border-btn).
    /// </summary>
    public string? BorderWidthBtn { get; set; }

    /// <summary>
    /// Controls the border width of tabs (--tab-border).
    /// </summary>
    public string? TabBorderWidth { get; set; }

    /// <summary>
    /// Duration of the animation when a button is clicked (--animation-btn).
    /// </summary>
    public string? AnimationBtn { get; set; }

    /// <summary>
    /// Duration of the animation for inputs like checkboxes and toggles (--animation-input).
    /// </summary>
    public string? AnimationInput { get; set; }

    /// <summary>
    /// The scale transform when a button is clicked or focused (e.g., 0.95) (--btn-focus-scale).
    /// </summary>
    public string? BtnFocusScale { get; set; }
}
