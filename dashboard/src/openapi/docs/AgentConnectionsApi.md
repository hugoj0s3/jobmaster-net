# AgentConnectionsApi

All URIs are relative to *http://localhost*

|Method | HTTP request | Description|
|------------- | ------------- | -------------|
|[**jmApiClusterIdAgentConnectionsAgentConnectionIdGet**](#jmapiclusteridagentconnectionsagentconnectionidget) | **GET** /jm-api/{clusterId}/agent-connections/{agentConnectionId} | |
|[**jmApiClusterIdAgentConnectionsCountGet**](#jmapiclusteridagentconnectionscountget) | **GET** /jm-api/{clusterId}/agent-connections/count | |
|[**jmApiClusterIdAgentConnectionsGet**](#jmapiclusteridagentconnectionsget) | **GET** /jm-api/{clusterId}/agent-connections | |

# **jmApiClusterIdAgentConnectionsAgentConnectionIdGet**
> jmApiClusterIdAgentConnectionsAgentConnectionIdGet()


### Example

```typescript
import {
    AgentConnectionsApi,
    Configuration
} from './api';

const configuration = new Configuration();
const apiInstance = new AgentConnectionsApi(configuration);

let clusterId: string; // (default to undefined)
let agentConnectionId: string; // (default to undefined)

const { status, data } = await apiInstance.jmApiClusterIdAgentConnectionsAgentConnectionIdGet(
    clusterId,
    agentConnectionId
);
```

### Parameters

|Name | Type | Description  | Notes|
|------------- | ------------- | ------------- | -------------|
| **clusterId** | [**string**] |  | defaults to undefined|
| **agentConnectionId** | [**string**] |  | defaults to undefined|


### Return type

void (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
|**200** | OK |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **jmApiClusterIdAgentConnectionsCountGet**
> jmApiClusterIdAgentConnectionsCountGet()


### Example

```typescript
import {
    AgentConnectionsApi,
    Configuration
} from './api';

const configuration = new Configuration();
const apiInstance = new AgentConnectionsApi(configuration);

let clusterId: string; // (default to undefined)

const { status, data } = await apiInstance.jmApiClusterIdAgentConnectionsCountGet(
    clusterId
);
```

### Parameters

|Name | Type | Description  | Notes|
|------------- | ------------- | ------------- | -------------|
| **clusterId** | [**string**] |  | defaults to undefined|


### Return type

void (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
|**200** | OK |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **jmApiClusterIdAgentConnectionsGet**
> jmApiClusterIdAgentConnectionsGet()


### Example

```typescript
import {
    AgentConnectionsApi,
    Configuration
} from './api';

const configuration = new Configuration();
const apiInstance = new AgentConnectionsApi(configuration);

let clusterId: string; // (default to undefined)

const { status, data } = await apiInstance.jmApiClusterIdAgentConnectionsGet(
    clusterId
);
```

### Parameters

|Name | Type | Description  | Notes|
|------------- | ------------- | ------------- | -------------|
| **clusterId** | [**string**] |  | defaults to undefined|


### Return type

void (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
|**200** | OK |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

