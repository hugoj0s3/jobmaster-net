namespace JobMaster.Dashboard.Ioc.Selectors.Themes;

public interface IJobMasterDashboardPrimaryThemeSelector : 
    IJobMasterDashboardThemeBaseSelector,
    IJobMasterDashboardThemeColorSelector<IJobMasterDashboardPrimaryThemeSelector>,
    IJobMasterDashboardFontFamilySelector<IJobMasterDashboardPrimaryThemeSelector>
{
    IJobMasterDashboardPrimaryThemeSelector SetBorderRadii(string box = null, string btn = null, string badge = null);
}
