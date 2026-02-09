# RecurringSchedulesApi

All URIs are relative to *http://localhost*

|Method | HTTP request | Description|
|------------- | ------------- | -------------|
|[**jmApiClusterIdRecurringSchedulesCountGet**](#jmapiclusteridrecurringschedulescountget) | **GET** /jm-api/{clusterId}/recurring-schedules/count | |
|[**jmApiClusterIdRecurringSchedulesGet**](#jmapiclusteridrecurringschedulesget) | **GET** /jm-api/{clusterId}/recurring-schedules | |
|[**jmApiClusterIdRecurringSchedulesIdGet**](#jmapiclusteridrecurringschedulesidget) | **GET** /jm-api/{clusterId}/recurring-schedules/{id} | |

# **jmApiClusterIdRecurringSchedulesCountGet**
> jmApiClusterIdRecurringSchedulesCountGet()


### Example

```typescript
import {
    RecurringSchedulesApi,
    Configuration
} from './api';

const configuration = new Configuration();
const apiInstance = new RecurringSchedulesApi(configuration);

let clusterId: string; // (default to undefined)
let status: RecurringScheduleStatus; // (optional) (default to undefined)
let startAfterTo: string; // (optional) (default to undefined)
let startAfterFrom: string; // (optional) (default to undefined)
let endBeforeTo: string; // (optional) (default to undefined)
let endBeforeFrom: string; // (optional) (default to undefined)
let coverageUntil: string; // (optional) (default to undefined)
let isJobCancellationPending: boolean; // (optional) (default to undefined)
let canceledOrInactive: boolean; // (optional) (default to undefined)
let recurringScheduleType: RecurringScheduleType; // (optional) (default to undefined)
let jobDefinitionId: string; // (optional) (default to undefined)
let profileId: string; // (optional) (default to undefined)
let workerLane: string; // (optional) (default to undefined)
let metadataFiltersJson: string; // (optional) (default to undefined)
let countLimit: number; // (optional) (default to undefined)
let offset: number; // (optional) (default to undefined)

const { status, data } = await apiInstance.jmApiClusterIdRecurringSchedulesCountGet(
    clusterId,
    status,
    startAfterTo,
    startAfterFrom,
    endBeforeTo,
    endBeforeFrom,
    coverageUntil,
    isJobCancellationPending,
    canceledOrInactive,
    recurringScheduleType,
    jobDefinitionId,
    profileId,
    workerLane,
    metadataFiltersJson,
    countLimit,
    offset
);
```

### Parameters

|Name | Type | Description  | Notes|
|------------- | ------------- | ------------- | -------------|
| **clusterId** | [**string**] |  | defaults to undefined|
| **status** | **RecurringScheduleStatus** |  | (optional) defaults to undefined|
| **startAfterTo** | [**string**] |  | (optional) defaults to undefined|
| **startAfterFrom** | [**string**] |  | (optional) defaults to undefined|
| **endBeforeTo** | [**string**] |  | (optional) defaults to undefined|
| **endBeforeFrom** | [**string**] |  | (optional) defaults to undefined|
| **coverageUntil** | [**string**] |  | (optional) defaults to undefined|
| **isJobCancellationPending** | [**boolean**] |  | (optional) defaults to undefined|
| **canceledOrInactive** | [**boolean**] |  | (optional) defaults to undefined|
| **recurringScheduleType** | **RecurringScheduleType** |  | (optional) defaults to undefined|
| **jobDefinitionId** | [**string**] |  | (optional) defaults to undefined|
| **profileId** | [**string**] |  | (optional) defaults to undefined|
| **workerLane** | [**string**] |  | (optional) defaults to undefined|
| **metadataFiltersJson** | [**string**] |  | (optional) defaults to undefined|
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

# **jmApiClusterIdRecurringSchedulesGet**
> jmApiClusterIdRecurringSchedulesGet()


### Example

```typescript
import {
    RecurringSchedulesApi,
    Configuration
} from './api';

const configuration = new Configuration();
const apiInstance = new RecurringSchedulesApi(configuration);

let clusterId: string; // (default to undefined)
let status: RecurringScheduleStatus; // (optional) (default to undefined)
let startAfterTo: string; // (optional) (default to undefined)
let startAfterFrom: string; // (optional) (default to undefined)
let endBeforeTo: string; // (optional) (default to undefined)
let endBeforeFrom: string; // (optional) (default to undefined)
let coverageUntil: string; // (optional) (default to undefined)
let isJobCancellationPending: boolean; // (optional) (default to undefined)
let canceledOrInactive: boolean; // (optional) (default to undefined)
let recurringScheduleType: RecurringScheduleType; // (optional) (default to undefined)
let jobDefinitionId: string; // (optional) (default to undefined)
let profileId: string; // (optional) (default to undefined)
let workerLane: string; // (optional) (default to undefined)
let metadataFiltersJson: string; // (optional) (default to undefined)
let countLimit: number; // (optional) (default to undefined)
let offset: number; // (optional) (default to undefined)

const { status, data } = await apiInstance.jmApiClusterIdRecurringSchedulesGet(
    clusterId,
    status,
    startAfterTo,
    startAfterFrom,
    endBeforeTo,
    endBeforeFrom,
    coverageUntil,
    isJobCancellationPending,
    canceledOrInactive,
    recurringScheduleType,
    jobDefinitionId,
    profileId,
    workerLane,
    metadataFiltersJson,
    countLimit,
    offset
);
```

### Parameters

|Name | Type | Description  | Notes|
|------------- | ------------- | ------------- | -------------|
| **clusterId** | [**string**] |  | defaults to undefined|
| **status** | **RecurringScheduleStatus** |  | (optional) defaults to undefined|
| **startAfterTo** | [**string**] |  | (optional) defaults to undefined|
| **startAfterFrom** | [**string**] |  | (optional) defaults to undefined|
| **endBeforeTo** | [**string**] |  | (optional) defaults to undefined|
| **endBeforeFrom** | [**string**] |  | (optional) defaults to undefined|
| **coverageUntil** | [**string**] |  | (optional) defaults to undefined|
| **isJobCancellationPending** | [**boolean**] |  | (optional) defaults to undefined|
| **canceledOrInactive** | [**boolean**] |  | (optional) defaults to undefined|
| **recurringScheduleType** | **RecurringScheduleType** |  | (optional) defaults to undefined|
| **jobDefinitionId** | [**string**] |  | (optional) defaults to undefined|
| **profileId** | [**string**] |  | (optional) defaults to undefined|
| **workerLane** | [**string**] |  | (optional) defaults to undefined|
| **metadataFiltersJson** | [**string**] |  | (optional) defaults to undefined|
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

# **jmApiClusterIdRecurringSchedulesIdGet**
> jmApiClusterIdRecurringSchedulesIdGet()


### Example

```typescript
import {
    RecurringSchedulesApi,
    Configuration
} from './api';

const configuration = new Configuration();
const apiInstance = new RecurringSchedulesApi(configuration);

let clusterId: string; // (default to undefined)
let id: string; // (default to undefined)

const { status, data } = await apiInstance.jmApiClusterIdRecurringSchedulesIdGet(
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

