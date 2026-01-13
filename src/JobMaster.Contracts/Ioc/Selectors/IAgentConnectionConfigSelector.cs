namespace JobMaster.Contracts.Ioc.Selectors;

public interface IAgentConnectionConfigSelector
{
    
    public IAgentConnectionConfigSelector AgentConnName(string agentConnName);
    public IAgentConnectionConfigSelector AgentRepoType(string repoType);
    public IAgentConnectionConfigSelector AgentConnString(string connString);
    public IAgentConnectionConfigSelector AgentDbOperationThrottleLimit(int limit);
}