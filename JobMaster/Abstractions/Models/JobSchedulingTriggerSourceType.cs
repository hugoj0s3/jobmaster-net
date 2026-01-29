namespace JobMaster.Abstractions.Models;

public enum JobSchedulingTriggerSourceType
{
    Once = 1,
    StaticRecurring = 2,
    DynamicRecurring = 3,
}