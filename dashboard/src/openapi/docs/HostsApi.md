# HostsApi

All URIs are relative to *http://localhost*

|Method | HTTP request | Description|
|------------- | ------------- | -------------|
|[**jmApiClusterIdHostsCountGet**](#jmapiclusteridhostscountget) | **GET** /jm-api/{clusterId}/hosts/count | |
|[**jmApiClusterIdHostsGet**](#jmapiclusteridhostsget) | **GET** /jm-api/{clusterId}/hosts | |
|[**jmApiClusterIdHostsHostIdGet**](#jmapiclusteridhostshostidget) | **GET** /jm-api/{clusterId}/hosts/{hostId} | |

# **jmApiClusterIdHostsCountGet**
> jmApiClusterIdHostsCountGet()


### Example

```typescript
import {
    HostsApi,
    Configuration
} from './api';

const configuration = new Configuration();
const apiInstance = new HostsApi(configuration);

let clusterId: string; // (default to undefined)

const { status, data } = await apiInstance.jmApiClusterIdHostsCountGet(
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

# **jmApiClusterIdHostsGet**
> jmApiClusterIdHostsGet()


### Example

```typescript
import {
    HostsApi,
    Configuration
} from './api';

const configuration = new Configuration();
const apiInstance = new HostsApi(configuration);

let clusterId: string; // (default to undefined)

const { status, data } = await apiInstance.jmApiClusterIdHostsGet(
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

# **jmApiClusterIdHostsHostIdGet**
> jmApiClusterIdHostsHostIdGet()


### Example

```typescript
import {
    HostsApi,
    Configuration
} from './api';

const configuration = new Configuration();
const apiInstance = new HostsApi(configuration);

let clusterId: string; // (default to undefined)
let hostId: string; // (default to undefined)

const { status, data } = await apiInstance.jmApiClusterIdHostsHostIdGet(
    clusterId,
    hostId
);
```

### Parameters

|Name | Type | Description  | Notes|
|------------- | ------------- | ------------- | -------------|
| **clusterId** | [**string**] |  | defaults to undefined|
| **hostId** | [**string**] |  | defaults to undefined|


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

