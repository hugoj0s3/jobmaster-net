using JobMaster.Sdk.Abstractions.Keys;

namespace JobMaster.SqlBase;

internal class SqlBaseConfigKeys
{
    public readonly static JobMasterNamespaceUniqueKey NamespaceUniqueKey = 
        new("JobMaster.SqlBase", "7a7d3e5b-6c9b-47e2-9f8a-8a0d8b0e3f2c");
    
    public static string TablePrefixKey => "TablePrefix";
    
    public static string DisableAutoProvisionSchemaKey => "DisableAutoProvisionSchema";
        
}