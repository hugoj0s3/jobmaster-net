using JobMaster.Sdk.Abstractions.Config;

namespace JobMaster.Sdk.Abstractions.Extensions;

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