namespace JobMaster.Contracts.Models;

public enum JobSchedulingSourceType
{
    Once = 1,
        
    StaticRecurring = 2,

    DynamicRecurring = 3,
}