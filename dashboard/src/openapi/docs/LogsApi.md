# LogsApi

All URIs are relative to *http://localhost*

|Method | HTTP request | Description|
|------------- | ------------- | -------------|
|[**jmApiClusterIdLogsCountGet**](#jmapiclusteridlogscountget) | **GET** /jm-api/{clusterId}/logs/count | |
|[**jmApiClusterIdLogsGet**](#jmapiclusteridlogsget) | **GET** /jm-api/{clusterId}/logs | |
|[**jmApiClusterIdLogsIdGet**](#jmapiclusteridlogsidget) | **GET** /jm-api/{clusterId}/logs/{id} | |

# **jmApiClusterIdLogsCountGet**
> jmApiClusterIdLogsCountGet()


### Example

```typescript
import {
    LogsApi,
    Configuration
} from './api';

const configuration = new Configuration();
const apiInstance = new LogsApi(configuration);

let clusterId: string; // (default to undefined)
let level: ApiJobMasterLogLevel; // (optional) (default to undefined)
let subjectType: ApiJobMasterLogSubjectType; // (optional) (default to undefined)
let subjectId: string; // (optional) (default to undefined)
let fromTimestamp: string; // (optional) (default to undefined)
let toTimestamp: string; // (optional) (default to undefined)
let keyword: string; // (optional) (default to undefined)
let countLimit: number; // (optional) (default to undefined)
let offset: number; // (optional) (default to undefined)

const { status, data } = await apiInstance.jmApiClusterIdLogsCountGet(
    clusterId,
    level,
    subjectType,
    subjectId,
    fromTimestamp,
    toTimestamp,
    keyword,
    countLimit,
    offset
);
```

### Parameters

|Name | Type | Description  | Notes|
|------------- | ------------- | ------------- | -------------|
| **clusterId** | [**string**] |  | defaults to undefined|
| **level** | **ApiJobMasterLogLevel** |  | (optional) defaults to undefined|
| **subjectType** | **ApiJobMasterLogSubjectType** |  | (optional) defaults to undefined|
| **subjectId** | [**string**] |  | (optional) defaults to undefined|
| **fromTimestamp** | [**string**] |  | (optional) defaults to undefined|
| **toTimestamp** | [**string**] |  | (optional) defaults to undefined|
| **keyword** | [**string**] |  | (optional) defaults to undefined|
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

# **jmApiClusterIdLogsGet**
> jmApiClusterIdLogsGet()


### Example

```typescript
import {
    LogsApi,
    Configuration
} from './api';

const configuration = new Configuration();
const apiInstance = new LogsApi(configuration);

let clusterId: string; // (default to undefined)
let level: ApiJobMasterLogLevel; // (optional) (default to undefined)
let subjectType: ApiJobMasterLogSubjectType; // (optional) (default to undefined)
let subjectId: string; // (optional) (default to undefined)
let fromTimestamp: string; // (optional) (default to undefined)
let toTimestamp: string; // (optional) (default to undefined)
let keyword: string; // (optional) (default to undefined)
let countLimit: number; // (optional) (default to undefined)
let offset: number; // (optional) (default to undefined)

const { status, data } = await apiInstance.jmApiClusterIdLogsGet(
    clusterId,
    level,
    subjectType,
    subjectId,
    fromTimestamp,
    toTimestamp,
    keyword,
    countLimit,
    offset
);
```

### Parameters

|Name | Type | Description  | Notes|
|------------- | ------------- | ------------- | -------------|
| **clusterId** | [**string**] |  | defaults to undefined|
| **level** | **ApiJobMasterLogLevel** |  | (optional) defaults to undefined|
| **subjectType** | **ApiJobMasterLogSubjectType** |  | (optional) defaults to undefined|
| **subjectId** | [**string**] |  | (optional) defaults to undefined|
| **fromTimestamp** | [**string**] |  | (optional) defaults to undefined|
| **toTimestamp** | [**string**] |  | (optional) defaults to undefined|
| **keyword** | [**string**] |  | (optional) defaults to undefined|
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

# **jmApiClusterIdLogsIdGet**
> jmApiClusterIdLogsIdGet()


### Example

```typescript
import {
    LogsApi,
    Configuration
} from './api';

const configuration = new Configuration();
const apiInstance = new LogsApi(configuration);

let clusterId: string; // (default to undefined)
let id: string; // (default to undefined)

const { status, data } = await apiInstance.jmApiClusterIdLogsIdGet(
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

