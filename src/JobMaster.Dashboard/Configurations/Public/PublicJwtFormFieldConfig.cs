namespace JobMaster.Dashboard.Configurations.Public;

public class PublicJwtFormFieldConfig
{
    public string Id { get; set; } = string.Empty;
    public string Label { get; set; } = string.Empty;
    public string Type { get; set; } = string.Empty;
    public bool IsRequired { get; set; }
    public string? DefaultValue { get; set; }
    public bool Disabled { get; set; }
}
