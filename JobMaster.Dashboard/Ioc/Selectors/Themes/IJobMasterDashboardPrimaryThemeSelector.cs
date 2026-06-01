namespace JobMaster.Dashboard.Ioc.Selectors.Themes;

public interface IJobMasterDashboardPrimaryThemeSelector : 
    IJobMasterDashboardThemeBaseSelector,
    IJobMasterDashboardThemeColorSelector<IJobMasterDashboardPrimaryThemeSelector>,
    IJobMasterDashboardFontFamilySelector<IJobMasterDashboardPrimaryThemeSelector>
{
    /// <summary>
    /// Sets DaisyUI v5 border radii. All parameters are optional CSS length values (e.g. <c>"0.75rem"</c>).
    /// </summary>
    /// <param name="box">Radius for box-like components: Cards, Modals (<c>--radius-box</c>).</param>
    /// <param name="selector">Radius for selector-like components: Buttons, Dropdowns (<c>--radius-selector</c>).</param>
    /// <param name="field">Radius for field-like components: Inputs, Textareas (<c>--radius-field</c>).</param>
    IJobMasterDashboardPrimaryThemeSelector SetBorderRadii(string? box = null, string? selector = null, string? field = null);
}
