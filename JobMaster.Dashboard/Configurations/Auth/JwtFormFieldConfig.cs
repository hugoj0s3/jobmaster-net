namespace JobMaster.Dashboard.Configurations.Auth;

internal sealed class JwtFormFieldConfig
{
    public string Id { get; set; } = string.Empty;
    public string Label { get; set; } = string.Empty;
    public DashboardJwtFormFieldType Type { get; set; } = DashboardJwtFormFieldType.Text;
    public bool IsRequired { get; set; } = true;
    public string? DefaultValue { get; set; }
    public bool Disabled { get; set; } = false;
}
