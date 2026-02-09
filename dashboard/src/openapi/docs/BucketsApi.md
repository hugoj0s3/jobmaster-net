# BucketsApi

All URIs are relative to *http://localhost*

|Method | HTTP request | Description|
|------------- | ------------- | -------------|
|[**jmApiClusterIdBucketsBucketIdGet**](#jmapiclusteridbucketsbucketidget) | **GET** /jm-api/{clusterId}/buckets/{bucketId} | |
|[**jmApiClusterIdBucketsCountGet**](#jmapiclusteridbucketscountget) | **GET** /jm-api/{clusterId}/buckets/count | |
|[**jmApiClusterIdBucketsGet**](#jmapiclusteridbucketsget) | **GET** /jm-api/{clusterId}/buckets | |

# **jmApiClusterIdBucketsBucketIdGet**
> jmApiClusterIdBucketsBucketIdGet()


### Example

```typescript
import {
    BucketsApi,
    Configuration
} from './api';

const configuration = new Configuration();
const apiInstance = new BucketsApi(configuration);

let clusterId: string; // (default to undefined)
let bucketId: string; // (default to undefined)

const { status, data } = await apiInstance.jmApiClusterIdBucketsBucketIdGet(
    clusterId,
    bucketId
);
```

### Parameters

|Name | Type | Description  | Notes|
|------------- | ------------- | ------------- | -------------|
| **clusterId** | [**string**] |  | defaults to undefined|
| **bucketId** | [**string**] |  | defaults to undefined|


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

# **jmApiClusterIdBucketsCountGet**
> jmApiClusterIdBucketsCountGet()


### Example

```typescript
import {
    BucketsApi,
    Configuration
} from './api';

const configuration = new Configuration();
const apiInstance = new BucketsApi(configuration);

let clusterId: string; // (default to undefined)
let agentConnectionId: string; // (optional) (default to undefined)
let priority: JobMasterPriority; // (optional) (default to undefined)
let status: BucketStatus; // (optional) (default to undefined)
let agentWorkerId: string; // (optional) (default to undefined)
let workerLane: string; // (optional) (default to undefined)

const { status, data } = await apiInstance.jmApiClusterIdBucketsCountGet(
    clusterId,
    agentConnectionId,
    priority,
    status,
    agentWorkerId,
    workerLane
);
```

### Parameters

|Name | Type | Description  | Notes|
|------------- | ------------- | ------------- | -------------|
| **clusterId** | [**string**] |  | defaults to undefined|
| **agentConnectionId** | [**string**] |  | (optional) defaults to undefined|
| **priority** | **JobMasterPriority** |  | (optional) defaults to undefined|
| **status** | **BucketStatus** |  | (optional) defaults to undefined|
| **agentWorkerId** | [**string**] |  | (optional) defaults to undefined|
| **workerLane** | [**string**] |  | (optional) defaults to undefined|


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

# **jmApiClusterIdBucketsGet**
> jmApiClusterIdBucketsGet()


### Example

```typescript
import {
    BucketsApi,
    Configuration
} from './api';

const configuration = new Configuration();
const apiInstance = new BucketsApi(configuration);

let clusterId: string; // (default to undefined)
let agentConnectionId: string; // (optional) (default to undefined)
let priority: JobMasterPriority; // (optional) (default to undefined)
let status: BucketStatus; // (optional) (default to undefined)
let agentWorkerId: string; // (optional) (default to undefined)
let workerLane: string; // (optional) (default to undefined)

const { status, data } = await apiInstance.jmApiClusterIdBucketsGet(
    clusterId,
    agentConnectionId,
    priority,
    status,
    agentWorkerId,
    workerLane
);
```

### Parameters

|Name | Type | Description  | Notes|
|------------- | ------------- | ------------- | -------------|
| **clusterId** | [**string**] |  | defaults to undefined|
| **agentConnectionId** | [**string**] |  | (optional) defaults to undefined|
| **priority** | **JobMasterPriority** |  | (optional) defaults to undefined|
| **status** | **BucketStatus** |  | (optional) defaults to undefined|
| **agentWorkerId** | [**string**] |  | (optional) defaults to undefined|
| **workerLane** | [**string**] |  | (optional) defaults to undefined|


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

