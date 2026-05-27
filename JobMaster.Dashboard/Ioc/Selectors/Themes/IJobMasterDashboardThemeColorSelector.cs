namespace JobMaster.Dashboard.Ioc.Selectors.Themes;

public interface IJobMasterDashboardThemeColorSelector<out TReturn>
{
    TReturn Primary(string color, string content = null);
    TReturn Secondary(string color, string content = null);
    TReturn Accent(string color, string content = null);
    TReturn Neutral(string color, string content = null);
    TReturn BaseColors(string base100 = null, string base200 = null, string base300 = null, string baseContent = null);
    TReturn Info(string color, string content = null);
    TReturn Success(string color, string content = null);
    TReturn Warning(string color, string content = null);
    TReturn Error(string color, string content = null);
}
