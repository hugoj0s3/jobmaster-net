using System.Collections.Concurrent;
using JobMaster.Internals;
using JobMaster.Sdk.Abstractions.Models.Logs;

namespace JobMaster.Sdk.Abstractions.Config;

public class JobMasterClusterConnectionConfig
{
    private static readonly object StaticLock = new();
    private readonly object InstanceLock = new();
    
    private static readonly ISet<JobMasterClusterConnectionConfig> ClusterConfigs = new HashSet<JobMasterClusterConnectionConfig>();
    
    public static JobMasterClusterConnectionConfig? Default
    {
        get
        {
            return DefaultConfig;
        }
        private set
        {
            lock (StaticLock)
            {
                DefaultConfig = value;
            }
        }
    }

    private static JobMasterClusterConnectionConfig? DefaultConfig;
    
    private JobMasterClusterConnectionConfig(string clusterId, string repositoryTypeId, string connectionString) 
    {
        ClusterId = clusterId;
        RepositoryTypeId = repositoryTypeId;
        ConnectionString = connectionString;
    }
    
    private ConcurrentDictionary<string, JobMasterAgentConnectionConfig> AgentConnectionConfigs { get; } 
        = new();
    
    public JobMasterConfigDictionary AdditionalConnConfig { get; private set; } = new ();
    
    public static int ClusterCount
    {
        get
        {
            return ClusterConfigs.Count;
        }
    }
    
    public string ClusterId { get;  }
    public string ConnectionString { get; private set; }
    public string RepositoryTypeId { get; private set; }
    
    public Action<LogItem>? MirrorLog { get; private set; }
    
    public bool IsActive { get; private set; }
    
    public int? RuntimeDbOperationThrottleLimit { get; private set; }

    public void SetRuntimeDbOperationThrottleLimit(int? value)
    {
        RuntimeDbOperationThrottleLimit = value;
    }
    
    public void SetMirrorLog(Action<LogItem>? mirrorLog)
    {
        MirrorLog = mirrorLog;
    }

    public void AddAgentConnectionString(
        string name, 
        string connectionString, 
        string repositoryTypeId, 
        JobMasterConfigDictionary? additionalConnConfig = null,
        int? runtimeDbOperationThrottleLimit = null)
    {
        lock (InstanceLock)
        {
            if (IsActive)
            {
                throw new InvalidOperationException("Cannot modify configuration while the cluster is active and running.");
            }
            
            if (string.IsNullOrEmpty(name) || string.IsNullOrEmpty(connectionString))
            {
                throw new ArgumentNullException("UniqueName or ConnectionString cannot be null or empty.");
            }
            
            if (TryGetAgentConnectionConfig(name) != null)
            {
                throw new ArgumentException($"Agent connection string '{name}' already exists.", nameof(name));
            }
            
            var agentConnConfig = new JobMasterAgentConnectionConfig(this.ClusterId, name, connectionString, repositoryTypeId, additionalConnConfig, runtimeDbOperationThrottleLimit);
            var id = agentConnConfig.Id;
            AgentConnectionConfigs[id] = agentConnConfig;
        }
            
    }
    
    public void SetJobMasterConfigDictionary(JobMasterConfigDictionary config)
    {
        lock (InstanceLock)
        {
            if (IsActive)
            {
                throw new InvalidOperationException("Cannot modify configuration while the cluster is active and running.");
            }
            
            AdditionalConnConfig = config;
        }
    }

    public JobMasterAgentConnectionConfig? TryGetAgentConnectionConfig(string idOrName)
    {
        var id = $"{ClusterId}:{idOrName}";
        if (!AgentConnectionConfigs.TryGetValue(id, out var agentConnectionString) && 
            !AgentConnectionConfigs.TryGetValue(idOrName, out agentConnectionString))
        {
            return null;
        }
            
        return agentConnectionString;
    }
    
    public JobMasterAgentConnectionConfig GetAgentConnectionConfig(string idOrName) 
    {
        var agentConnectionString = TryGetAgentConnectionConfig(idOrName);
        if (agentConnectionString == null)
        {
            throw new KeyNotFoundException($"Agent connection string '{idOrName}' not found.");
        }
        
        return agentConnectionString;
    }
    
    public IList<JobMasterAgentConnectionConfig> GetAllAgentConnectionConfigs()
    {
        return AgentConnectionConfigs.Values.ToList();
    }

    /// <summary>
    /// Activates this cluster configuration, preventing any further modifications.
    /// Call this when the cluster starts running. This operation cannot be undone.
    /// </summary>
    public void Activate()
    {
        lock (InstanceLock)
        {
            IsActive = true;
            
            AdditionalConnConfig.LockChanges();
            
            foreach (var agentConnConfig in AgentConnectionConfigs.Values)
            {
                agentConnConfig.AdditionalConnConfig.LockChanges();
            }
        }
    }

    public override bool Equals(object? other)
    {
        if (other is null)
        {
            return false;
        }
        
        if (other is JobMasterClusterConnectionConfig clusterConfig)
        {
            return ClusterId == clusterConfig.ClusterId;
        }
        
        return false;
    }
    
    public override int GetHashCode()
    {
        return ClusterId.GetHashCode();
    }
    
    
    public static JobMasterClusterConnectionConfig Create(string clusterId, string repositoryTypeId, string connectionString, bool isDefault = false, int? runtimeDbOperationThrottleLimit = null)
    {
        if (!JobMasterStringUtils.IsValidForId(clusterId))
            throw new ArgumentException($"Invalid cluster ID format. Only letters, numbers, underscore (_), hyphen (-), and dot (.) are allowed. Received: '{clusterId}'", nameof(clusterId));

        lock (StaticLock)
        {
            if (TryGet(clusterId, includeInactive: true) != null)
            {
                throw new ArgumentException($"Cluster ID '{clusterId}' already exists.", nameof(clusterId));
            }
            
            var config = new JobMasterClusterConnectionConfig(clusterId, repositoryTypeId, connectionString);
            
            ClusterConfigs.Add(config);
            
            if (isDefault)
            {
                DefaultConfig = config;
            }
            
            config.RuntimeDbOperationThrottleLimit = runtimeDbOperationThrottleLimit;
            
            return config;
        }
    }
    
    public static JobMasterClusterConnectionConfig? TryGet(string clusterId, bool includeInactive = false)
    {
        return ClusterConfigs.Where(c => includeInactive || c.IsActive).FirstOrDefault(c => c.ClusterId == clusterId);
    }

    public static JobMasterClusterConnectionConfig Get(string clusterId, bool includeInactive = false)
    {
        var config = TryGet(clusterId, includeInactive);
        if (config == null)
        {
            throw new KeyNotFoundException($"Cluster config '{clusterId}' not found.");
        }

        return config;
    }

    public static void SetDefaultConfig(string clusterId)
    {
        lock (StaticLock)
        {
            var config = TryGet(clusterId, includeInactive: true);
            DefaultConfig = config ?? throw new KeyNotFoundException($"Cluster config '{clusterId}' not found or inactive.");
        }
    }

    public static IList<JobMasterClusterConnectionConfig> GetActiveConfigs()
    {
        return ClusterConfigs.Where(c => c.IsActive).ToList();
    }

    public static IList<JobMasterClusterConnectionConfig> GetAllConfigs()
    {
        return ClusterConfigs.ToList();
    }
}