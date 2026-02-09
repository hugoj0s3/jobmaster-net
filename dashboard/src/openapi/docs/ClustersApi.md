# ClustersApi

All URIs are relative to *http://localhost*

|Method | HTTP request | Description|
|------------- | ------------- | -------------|
|[**jmApiClustersClusterIdGet**](#jmapiclustersclusteridget) | **GET** /jm-api/clusters/{clusterId} | |
|[**jmApiClustersCountGet**](#jmapiclusterscountget) | **GET** /jm-api/clusters/count | |
|[**jmApiClustersIdsGet**](#jmapiclustersidsget) | **GET** /jm-api/clusters/ids | |

# **jmApiClustersClusterIdGet**
> jmApiClustersClusterIdGet()


### Example

```typescript
import {
    ClustersApi,
    Configuration
} from './api';

const configuration = new Configuration();
const apiInstance = new ClustersApi(configuration);

let clusterId: string; // (default to undefined)

const { status, data } = await apiInstance.jmApiClustersClusterIdGet(
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

# **jmApiClustersCountGet**
> jmApiClustersCountGet()


### Example

```typescript
import {
    ClustersApi,
    Configuration
} from './api';

const configuration = new Configuration();
const apiInstance = new ClustersApi(configuration);

const { status, data } = await apiInstance.jmApiClustersCountGet();
```

### Parameters
This endpoint does not have any parameters.


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

# **jmApiClustersIdsGet**
> jmApiClustersIdsGet()


### Example

```typescript
import {
    ClustersApi,
    Configuration
} from './api';

const configuration = new Configuration();
const apiInstance = new ClustersApi(configuration);

const { status, data } = await apiInstance.jmApiClustersIdsGet();
```

### Parameters
This endpoint does not have any parameters.


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

