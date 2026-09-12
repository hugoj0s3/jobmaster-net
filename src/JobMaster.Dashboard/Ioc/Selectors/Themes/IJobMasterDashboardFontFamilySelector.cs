namespace JobMaster.Dashboard.Ioc.Selectors.Themes;

public interface IJobMasterDashboardFontFamilySelector<out TReturn>
{
    /// <summary>
    /// Sets the sans-serif font family stack (<c>--font-sans</c>).
    /// Pass the font name first, followed by fallbacks, e.g. <c>["Geist", "ui-sans-serif"]</c>.
    /// If the font requires a stylesheet to be loaded (Google Fonts, Bunny Fonts, self-hosted),
    /// pass its URL via <paramref name="fontUrl"/> and the dashboard will inject it at runtime.
    /// </summary>
    /// <param name="fontFamilies">CSS font-family stack. First entry is the primary font.</param>
    /// <param name="fontUrl">
    /// Optional URL of a CSS stylesheet that loads the font, e.g.
    /// <c>"https://fonts.googleapis.com/css2?family=Geist:wght@300;400;800&amp;display=swap"</c>.
    /// </param>
    TReturn SetFontSans(string[] fontFamilies, string? fontUrl = null);

    /// <summary>
    /// Sets the monospace font family stack (<c>--font-mono</c>).
    /// Pass the font name first, followed by fallbacks, e.g. <c>["Fira Code", "monospace"]</c>.
    /// If the font requires a stylesheet to be loaded (Google Fonts, Bunny Fonts, self-hosted),
    /// pass its URL via <paramref name="fontUrl"/> and the dashboard will inject it at runtime.
    /// </summary>
    /// <param name="fontFamilies">CSS font-family stack. First entry is the primary font.</param>
    /// <param name="fontUrl">
    /// Optional URL of a CSS stylesheet that loads the font, e.g.
    /// <c>"https://fonts.googleapis.com/css2?family=Fira+Code:wght@400;700&amp;display=swap"</c>.
    /// </param>
    TReturn SetFontMono(string[] fontFamilies, string? fontUrl = null);
}
