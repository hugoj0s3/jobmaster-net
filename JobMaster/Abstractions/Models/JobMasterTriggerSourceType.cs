namespace JobMaster.Abstractions.Models;

public enum JobMasterTriggerSourceType
{
    Once = 1,
    StaticRecurring = 2,
    DynamicRecurring = 3,
}

public static class JobMasterTriggerSourceTypeExtension
{
    public static bool IsRecurringTrigger(this JobMasterTriggerSourceType sourceType)
    {
        return sourceType == JobMasterTriggerSourceType.StaticRecurring || sourceType == JobMasterTriggerSourceType.DynamicRecurring;
    }
}
