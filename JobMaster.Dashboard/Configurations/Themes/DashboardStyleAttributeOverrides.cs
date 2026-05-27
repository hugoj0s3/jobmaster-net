namespace JobMaster.Dashboard.Configurations.Themes;

public class DashboardStyleAttributeOverrides
{
    /// <summary>
    /// Controls the sans-serif font family stack (--font-sans).
    /// </summary>
    public string[]? FontFamily { get; set; }

    /// <summary>
    /// Controls the monospace font family stack (--font-mono).
    /// </summary>
    public string[]? FontFamilyMono { get; set; }

    /// <summary>
    /// Controls the serif font family stack (--font-serif).
    /// </summary>
    public string[]? FontFamilySerif { get; set; }

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
