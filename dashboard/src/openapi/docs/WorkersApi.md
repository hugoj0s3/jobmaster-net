# WorkersApi

All URIs are relative to *http://localhost*

|Method | HTTP request | Description|
|------------- | ------------- | -------------|
|[**jmApiClusterIdWorkersCountGet**](#jmapiclusteridworkerscountget) | **GET** /jm-api/{clusterId}/workers/count | |
|[**jmApiClusterIdWorkersGet**](#jmapiclusteridworkersget) | **GET** /jm-api/{clusterId}/workers | |
|[**jmApiClusterIdWorkersWorkerIdGet**](#jmapiclusteridworkersworkeridget) | **GET** /jm-api/{clusterId}/workers/{workerId} | |

# **jmApiClusterIdWorkersCountGet**
> jmApiClusterIdWorkersCountGet()


### Example

```typescript
import {
    WorkersApi,
    Configuration
} from './api';

const configuration = new Configuration();
const apiInstance = new WorkersApi(configuration);

let clusterId: string; // (default to undefined)
let agentConnectionId: string; // (optional) (default to undefined)
let workerLane: string; // (optional) (default to undefined)
let status: AgentWorkerStatus; // (optional) (default to undefined)
let mode: AgentWorkerMode; // (optional) (default to undefined)
let isAlive: boolean; // (optional) (default to undefined)

const { status, data } = await apiInstance.jmApiClusterIdWorkersCountGet(
    clusterId,
    agentConnectionId,
    workerLane,
    status,
    mode,
    isAlive
);
```

### Parameters

|Name | Type | Description  | Notes|
|------------- | ------------- | ------------- | -------------|
| **clusterId** | [**string**] |  | defaults to undefined|
| **agentConnectionId** | [**string**] |  | (optional) defaults to undefined|
| **workerLane** | [**string**] |  | (optional) defaults to undefined|
| **status** | **AgentWorkerStatus** |  | (optional) defaults to undefined|
| **mode** | **AgentWorkerMode** |  | (optional) defaults to undefined|
| **isAlive** | [**boolean**] |  | (optional) defaults to undefined|


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

# **jmApiClusterIdWorkersGet**
> jmApiClusterIdWorkersGet()


### Example

```typescript
import {
    WorkersApi,
    Configuration
} from './api';

const configuration = new Configuration();
const apiInstance = new WorkersApi(configuration);

let clusterId: string; // (default to undefined)
let agentConnectionId: string; // (optional) (default to undefined)
let workerLane: string; // (optional) (default to undefined)
let status: AgentWorkerStatus; // (optional) (default to undefined)
let mode: AgentWorkerMode; // (optional) (default to undefined)
let isAlive: boolean; // (optional) (default to undefined)

const { status, data } = await apiInstance.jmApiClusterIdWorkersGet(
    clusterId,
    agentConnectionId,
    workerLane,
    status,
    mode,
    isAlive
);
```

### Parameters

|Name | Type | Description  | Notes|
|------------- | ------------- | ------------- | -------------|
| **clusterId** | [**string**] |  | defaults to undefined|
| **agentConnectionId** | [**string**] |  | (optional) defaults to undefined|
| **workerLane** | [**string**] |  | (optional) defaults to undefined|
| **status** | **AgentWorkerStatus** |  | (optional) defaults to undefined|
| **mode** | **AgentWorkerMode** |  | (optional) defaults to undefined|
| **isAlive** | [**boolean**] |  | (optional) defaults to undefined|


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

# **jmApiClusterIdWorkersWorkerIdGet**
> jmApiClusterIdWorkersWorkerIdGet()


### Example

```typescript
import {
    WorkersApi,
    Configuration
} from './api';

const configuration = new Configuration();
const apiInstance = new WorkersApi(configuration);

let clusterId: string; // (default to undefined)
let workerId: string; // (default to undefined)

const { status, data } = await apiInstance.jmApiClusterIdWorkersWorkerIdGet(
    clusterId,
    workerId
);
```

### Parameters

|Name | Type | Description  | Notes|
|------------- | ------------- | ------------- | -------------|
| **clusterId** | [**string**] |  | defaults to undefined|
| **workerId** | [**string**] |  | defaults to undefined|


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

