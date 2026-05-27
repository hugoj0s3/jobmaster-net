namespace JobMaster.Dashboard.Ioc.Selectors.Themes;

public interface IJobMasterDashboardFontFamilySelector<out TReturn>
{
    TReturn SetFontSans(params string[] fontFamilies);
    TReturn SetFontMono(params string[] fontFamilies);
    TReturn SetFontSerif(params string[] fontFamilies);
}