namespace JobMaster.Sdk.Contracts.Keys;

public struct JobMasterNamespaceUniqueKey
{
    public string NamespaceKey { get; private set; }
    public Guid StaticId { get; private set; }

    public JobMasterNamespaceUniqueKey(string namespaceKey, Guid staticId)
    {
        NamespaceKey = namespaceKey;
        StaticId = staticId;
    }

    public JobMasterNamespaceUniqueKey(string namespaceKey, string staticId) : this(namespaceKey, Guid.Parse(staticId))
    {
    }
    
    public override string ToString()
    {
        return $"{NamespaceKey}.{StaticId:N}";
    }

    public static JobMasterNamespaceUniqueKey Parse(string value)
    {
        var split = value.Split('.');
        if (split.Length != 2)
        {
            throw new ArgumentException($"Invalid key format. Received: '{value}'");
        }

        return new JobMasterNamespaceUniqueKey(split[0], Guid.Parse(split[1]));
    }
    
    public static JobMasterNamespaceUniqueKey? TryParse(string value) 
    {
        var split = value.Split('.');
        if (split.Length != 2)
        {
            return null;
        }
        
        if (!Guid.TryParse(split[1], out var staticId)) 
        {
            return null;
        }
        
        return new JobMasterNamespaceUniqueKey(split[0], staticId);
    }
}