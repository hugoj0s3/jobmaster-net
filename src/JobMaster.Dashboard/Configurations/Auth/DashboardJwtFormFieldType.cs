namespace JobMaster.Dashboard.Configurations.Auth;

public enum DashboardJwtFormFieldType
{
    Text = 1,
    Password = 2,
    Email = 3,
    Number = 4,
    Hidden = 5,
    TextArea = 6,
    Checkbox = 7

    // Future versions:
    // Select     = 8  — requires Options: string[] on JwtFormFieldConfig
    // Tel        = 9  — phone/MFA, niche
    // Otp        = 10 — digit-box rendering, non-trivial UI
    // FileUpload = 11 — certificate/key file upload, non-trivial handling
}
