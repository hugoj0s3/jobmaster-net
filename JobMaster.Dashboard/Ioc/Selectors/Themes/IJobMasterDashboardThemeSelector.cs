namespace JobMaster.Dashboard.Ioc.Selectors.Themes;

public interface IJobMasterDashboardThemeBaseSelector
{
    IJobMasterDashboardThemeSelector DefaultForClusterIds(params string[] clusterIds);
    IJobMasterDashboardThemeSelector DefaultForClusterId(string clusterIds);
    IJobMasterDashboardPrimaryThemeSelector MakePrimary();
}
public interface IJobMasterDashboardThemeSelector :
    IJobMasterDashboardThemeBaseSelector,
    IJobMasterDashboardThemeColorSelector<IJobMasterDashboardThemeSelector>
{

}
