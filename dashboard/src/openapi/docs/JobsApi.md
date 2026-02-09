# JobsApi

All URIs are relative to *http://localhost*

|Method | HTTP request | Description|
|------------- | ------------- | -------------|
|[**jmApiClusterIdJobsCountGet**](#jmapiclusteridjobscountget) | **GET** /jm-api/{clusterId}/jobs/count | |
|[**jmApiClusterIdJobsGet**](#jmapiclusteridjobsget) | **GET** /jm-api/{clusterId}/jobs | |
|[**jmApiClusterIdJobsIdGet**](#jmapiclusteridjobsidget) | **GET** /jm-api/{clusterId}/jobs/{id} | |

# **jmApiClusterIdJobsCountGet**
> jmApiClusterIdJobsCountGet()


### Example

```typescript
import {
    JobsApi,
    Configuration
} from './api';

const configuration = new Configuration();
const apiInstance = new JobsApi(configuration);

let clusterId: string; // (default to undefined)
let status: JobMasterJobStatus; // (optional) (default to undefined)
let scheduledTo: string; // (optional) (default to undefined)
let scheduledFrom: string; // (optional) (default to undefined)
let processDeadlineTo: string; // (optional) (default to undefined)
let recurringScheduleId: string; // (optional) (default to undefined)
let metadataFiltersJson: string; // (optional) (default to undefined)
let jobDefinitionId: string; // (optional) (default to undefined)
let workerLane: string; // (optional) (default to undefined)
let countLimit: number; // (optional) (default to undefined)
let offset: number; // (optional) (default to undefined)

const { status, data } = await apiInstance.jmApiClusterIdJobsCountGet(
    clusterId,
    status,
    scheduledTo,
    scheduledFrom,
    processDeadlineTo,
    recurringScheduleId,
    metadataFiltersJson,
    jobDefinitionId,
    workerLane,
    countLimit,
    offset
);
```

### Parameters

|Name | Type | Description  | Notes|
|------------- | ------------- | ------------- | -------------|
| **clusterId** | [**string**] |  | defaults to undefined|
| **status** | **JobMasterJobStatus** |  | (optional) defaults to undefined|
| **scheduledTo** | [**string**] |  | (optional) defaults to undefined|
| **scheduledFrom** | [**string**] |  | (optional) defaults to undefined|
| **processDeadlineTo** | [**string**] |  | (optional) defaults to undefined|
| **recurringScheduleId** | [**string**] |  | (optional) defaults to undefined|
| **metadataFiltersJson** | [**string**] |  | (optional) defaults to undefined|
| **jobDefinitionId** | [**string**] |  | (optional) defaults to undefined|
| **workerLane** | [**string**] |  | (optional) defaults to undefined|
| **countLimit** | [**number**] |  | (optional) defaults to undefined|
| **offset** | [**number**] |  | (optional) defaults to undefined|


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

# **jmApiClusterIdJobsGet**
> jmApiClusterIdJobsGet()


### Example

```typescript
import {
    JobsApi,
    Configuration
} from './api';

const configuration = new Configuration();
const apiInstance = new JobsApi(configuration);

let clusterId: string; // (default to undefined)
let status: JobMasterJobStatus; // (optional) (default to undefined)
let scheduledTo: string; // (optional) (default to undefined)
let scheduledFrom: string; // (optional) (default to undefined)
let processDeadlineTo: string; // (optional) (default to undefined)
let recurringScheduleId: string; // (optional) (default to undefined)
let metadataFiltersJson: string; // (optional) (default to undefined)
let jobDefinitionId: string; // (optional) (default to undefined)
let workerLane: string; // (optional) (default to undefined)
let countLimit: number; // (optional) (default to undefined)
let offset: number; // (optional) (default to undefined)

const { status, data } = await apiInstance.jmApiClusterIdJobsGet(
    clusterId,
    status,
    scheduledTo,
    scheduledFrom,
    processDeadlineTo,
    recurringScheduleId,
    metadataFiltersJson,
    jobDefinitionId,
    workerLane,
    countLimit,
    offset
);
```

### Parameters

|Name | Type | Description  | Notes|
|------------- | ------------- | ------------- | -------------|
| **clusterId** | [**string**] |  | defaults to undefined|
| **status** | **JobMasterJobStatus** |  | (optional) defaults to undefined|
| **scheduledTo** | [**string**] |  | (optional) defaults to undefined|
| **scheduledFrom** | [**string**] |  | (optional) defaults to undefined|
| **processDeadlineTo** | [**string**] |  | (optional) defaults to undefined|
| **recurringScheduleId** | [**string**] |  | (optional) defaults to undefined|
| **metadataFiltersJson** | [**string**] |  | (optional) defaults to undefined|
| **jobDefinitionId** | [**string**] |  | (optional) defaults to undefined|
| **workerLane** | [**string**] |  | (optional) defaults to undefined|
| **countLimit** | [**number**] |  | (optional) defaults to undefined|
| **offset** | [**number**] |  | (optional) defaults to undefined|


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

# **jmApiClusterIdJobsIdGet**
> jmApiClusterIdJobsIdGet()


### Example

```typescript
import {
    JobsApi,
    Configuration
} from './api';

const configuration = new Configuration();
const apiInstance = new JobsApi(configuration);

let clusterId: string; // (default to undefined)
let id: string; // (default to undefined)

const { status, data } = await apiInstance.jmApiClusterIdJobsIdGet(
    clusterId,
    id
);
```

### Parameters

|Name | Type | Description  | Notes|
|------------- | ------------- | ------------- | -------------|
| **clusterId** | [**string**] |  | defaults to undefined|
| **id** | [**string**] |  | defaults to undefined|


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

