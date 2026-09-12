using JobMaster.Sdk.Utils;

namespace JobMaster.Sdk.Abstractions.Keys;

internal abstract class JobMasterKeyManager
{
    protected virtual string BasePrefix() => JobMasterGlobalKey.Key.ToString();
    protected readonly string clusterPrefix;
    
    protected JobMasterKeyManager(string keyType, string clusterId)
    {
        if (string.IsNullOrWhiteSpace(keyType))
            throw new ArgumentException("Key type cannot be null or empty", nameof(keyType));
        
        if (!JobMasterStringUtils.IsValidForId(clusterId))
            throw new ArgumentException($"Invalid cluster ID format. Only letters, numbers, underscore (_), hyphen (-), and dot (.) are allowed. Received: '{clusterId}'", nameof(clusterId));
            
        clusterPrefix = $"{BasePrefix()}:{keyType}:{clusterId}";
    }
    
    /// <summary>
    /// Validates that a key follows the correct JobMaster key format
    /// </summary>
    /// <param name="key">The key to validate</param>
    /// <returns>True if the key format is valid</returns>
    public bool IsValidKeyFormat(string key)
    {
        if (string.IsNullOrWhiteSpace(key))
            return false;
            
        // Expected format: {BasePrefix}:{KeyType}:{ClusterId}:{Specific}
        var parts = key.Split(':');
        if (parts.Length < 4)
            return false;
        
        var basePrefix = parts[0];
        var keyType = parts[1];
        var clusterId = parts[2];
            
        // Validate base prefix
        if (basePrefix != BasePrefix())
            return false;
            
        // Validate KeyType - only letters, numbers, underscore, hyphen
        if (!JobMasterStringUtils.IsValidForId(keyType))
            return false;
            
        // Validate ClusterId - only letters, numbers, underscore, hyphen
        if (!JobMasterStringUtils.IsValidForId(clusterId))
            return false;
        
        var specific = parts.Length > 4 ? parts[3] : string.Empty;
        if (!string.IsNullOrWhiteSpace(specific) && !JobMasterStringUtils.IsValidForId(specific))
            return false;

        if (!key.StartsWith(clusterPrefix))
        {
            return false;
        }
            
        return true;
    }
    
    /// <summary>
    /// Validates that a key follows the correct JobMaster key format and throws an exception if invalid
    /// </summary>
    /// <param name="key">The key to validate</param>
    /// <exception cref="ArgumentException">Thrown when the key format is invalid</exception>
    public void ValidateKeyFormat(string key)
    {
        if (!IsValidKeyFormat(key))
        {
            throw new ArgumentException(
                $"Invalid JobMaster key format. Expected format: '{JobMasterGlobalKey.Key}:{{KeyType}}:{{ClusterId}}:{{Specific}}'. Received: '{key}'", 
                nameof(key));
        }
    }
    
    /// <summary>
    /// Creates a key with the cluster prefix and validates the result
    /// </summary>
    /// <param name="suffix">The suffix to append to the cluster prefix</param>
    /// <returns>A validated key</returns>
    protected string CreateKey(string suffix)
    {
        if (string.IsNullOrWhiteSpace(suffix))
            throw new ArgumentException("Key suffix cannot be null or empty", nameof(suffix));
            
        var key = $"{clusterPrefix}:{suffix}";
        ValidateKeyFormat(key);
        return key;
    }
}
