using JobMaster.Sdk.Contracts.Config;

namespace JobMaster.Sdk.Contracts.Extensions;

internal static class DictionaryExtensions
{
    internal static JobMasterConfigDictionary? ToJobMasterConfigDictionary(this IDictionary<string, object>? dictionary)
    {
        if (dictionary is null)
        {
            return null;
        }
        
        return new JobMasterConfigDictionary(dictionary);
    }
}