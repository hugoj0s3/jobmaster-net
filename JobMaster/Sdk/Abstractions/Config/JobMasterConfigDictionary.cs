using System.ComponentModel;
using System.Collections.Concurrent;
using JobMaster.Internals;
using JobMaster.Sdk.Abstractions.Keys;

namespace JobMaster.Sdk.Abstractions.Config;

[EditorBrowsable(EditorBrowsableState.Never)]
public class JobMasterConfigDictionary
{
    private readonly IDictionary<string, object> Config = new ConcurrentDictionary<string, object>();
    public JobMasterConfigDictionary() { }

    public JobMasterConfigDictionary(IDictionary<string, object> config)
    {
        foreach (var item in config)
        {
            if (item.Value is null)
            {
                continue;
            }
            
            if (!ValidateFullKey(item.Key))
            {
                throw new ArgumentException($"Invalid key format. Only letters, numbers, underscore (_), hyphen (-), and dot (.) are allowed. Received: '{item.Key}'", nameof(item.Key));
            }
            
            this.Config[item.Key] = item.Value;
        }
    }
    
    /// <summary>
    /// Locks the configuration dictionary to prevent any changes.
    /// </summary>
    public bool IsLocked { get; private set; } 

    public void SetConfig(JobMasterNamespaceUniqueKey namespaceKey, IDictionary<string, object> config)
    {
        if (IsLocked)
        {
            throw new InvalidOperationException("Configuration is locked and cannot be modified.");
        }
        
        foreach (var item in config)
        {
            if (!ValidateSubKey(item.Key))
            {
                throw new ArgumentException($"Invalid key format. Only letters, numbers, underscore (_), hyphen (-), and dot (.) are allowed. Received: '{item.Key}'", nameof(item.Key));
            }

            this.Config[$"{namespaceKey}:{item.Key}"] = item.Value;
        }
    }
    
    public IDictionary<string, object> GetConfig(JobMasterNamespaceUniqueKey namespaceKey)
    {
        var config = new Dictionary<string, object>();
        foreach (var item in Config)
        {
            if (item.Key.StartsWith(namespaceKey + ":"))
            {
                var key = item.Key.Substring(namespaceKey.ToString().Length + 1);
                config[key] = item.Value;
            }
        }
        
        return config;
    }
    
    public void RemoveConfig(JobMasterNamespaceUniqueKey namespaceKey) 
    {
        if (IsLocked) 
        {
            throw new InvalidOperationException("Configuration is locked and cannot be modified.");
        }
        
        foreach (var item in Config)
        {
            if (item.Key.StartsWith(namespaceKey + ":"))
            {
                Config.Remove(item.Key);
            }
        }
    }
    
    public void SetValue(JobMasterNamespaceUniqueKey namespaceKey, string key, object value) 
    {
        if (IsLocked) 
        {
            throw new InvalidOperationException("Configuration is locked and cannot be modified.");
        }
        
        if (!ValidateSubKey(key))
        {
            throw new ArgumentException($"Invalid key format. Only letters, numbers, underscore (_), hyphen (-), and dot (.) are allowed. Received: '{key}'", nameof(key));
        }
        
        this.Config[$"{namespaceKey}:{key}"] = value;
    }
    
    public void RemoveValue(JobMasterNamespaceUniqueKey namespaceKey, string key) 
    {
        if (IsLocked) 
        {
            throw new InvalidOperationException("Configuration is locked and cannot be modified.");
        }
        
        if (!ValidateSubKey(key))
        {
            throw new ArgumentException($"Invalid key format. Only letters, numbers, underscore (_), hyphen (-), and dot (.) are allowed. Received: '{key}'", nameof(key));
        }
        
        this.Config.Remove($"{namespaceKey}:{key}");
    }
    
    public object GetValue(JobMasterNamespaceUniqueKey namespaceKey, string key) 
    {
        if (!ValidateSubKey(key))
        {
            throw new ArgumentException($"Invalid key format. Only letters, numbers, underscore (_), hyphen (-), and dot (.) are allowed. Received: '{key}'", nameof(key));
        }
        
        return this.Config[$"{namespaceKey}:{key}"];
    }
    
    public T GetValue<T>(JobMasterNamespaceUniqueKey namespaceKey, string key) 
    {
        return (T)this.Config[$"{namespaceKey}:{key}"];
    }

    public object? TryGetValue(JobMasterNamespaceUniqueKey namespaceKey, string key)
    {
        if (!ValidateSubKey(key))
        {
            return null;
        }
        
        return this.Config.TryGetValue($"{namespaceKey}:{key}", out var value) ? value : null;
    }
    
    public T? TryGetValue<T>(JobMasterNamespaceUniqueKey namespaceKey, string key)
    {
        var value = TryGetValue(namespaceKey, key);
        if (value is null)
        {
            return default;
        }
        
        return (T)value;
    }

    public void LockChanges()
    {
        this.IsLocked = true;
    }

    public IDictionary<string, object> GetFullDictionary()
    {
        return new Dictionary<string, object>(this.Config);
    }


    private bool ValidateSubKey(string key)
    {
        return JobMasterStringUtils.IsValidForId(key);
    }

    private bool ValidateFullKey(string key)
    {
        var split = key.Split(':');
        if (split.Length != 2)
        {
            return false;
        }

        var namespaceKey = split[0];
        if (JobMasterNamespaceUniqueKey.TryParse(namespaceKey) is null)
        {
            return false;
        }

        var subKey = split[1];
        return ValidateSubKey(subKey);
    }
}