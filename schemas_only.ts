    schemas: {
        /**
         * Format: int32
         * @enum {integer}
         */
        AgentWorkerMode: 1 | 2 | 3 | 4;
        /**
         * Format: int32
         * @enum {integer}
         */
        AgentWorkerStatus: 0 | 1 | 2;
        ApiBucketModel: {
            clusterId?: string | null;
            id?: string | null;
            name?: string | null;
            agentConnectionId?: string | null;
            agentConnectionName?: string | null;
            agentWorkerId?: string | null;
            hostId?: string | null;
            hostDisplayName?: string | null;
            repositoryTypeId?: string | null;
            priority?: components["schemas"]["JobMasterPriority"];
            status?: components["schemas"]["BucketStatus"];
            /** Format: date-time */
            createdAt?: string;
            color?: string | null;
            workerLane?: string | null;
            /** Format: date-time */
            lastStatusChangeAt?: string;
        };
        ApiClusterModel: {
            clusterId?: string | null;
            repositoryTypeId?: string | null;
            /** Format: date-span */
            defaultJobTimeout?: string;
            /** Format: date-span */
            transientThreshold?: string;
            /** Format: int32 */
            defaultMaxOfRetryCount?: number;
            clusterMode?: components["schemas"]["ClusterMode"];
            /** Format: int32 */
            maxMessageByteSize?: number;
            ianaTimeZoneId?: string | null;
            /** Format: date-span */
            dataRetentionTtl?: string | null;
            additionalConfig?: {
                [key: string]: unknown;
            } | null;
        };
        ApiHostModel: {
            id?: string | null;
            displayName?: string | null;
            /** Format: double */
            cpuUsagePercent?: number | null;
            /** Format: int64 */
            memoryTotalBytes?: number | null;
            /** Format: int64 */
            memoryUsedBytes?: number | null;
            /** Format: int32 */
            threadCount?: number;
            /** Format: int32 */
            handleCount?: number;
        };
        /**
         * Format: int32
         * @enum {integer}
         */
        ApiJobMasterLogLevel: 0 | 1 | 2 | 3 | 4;
        /**
         * Format: int32
         * @enum {integer}
         */
        ApiJobMasterLogSubjectType: 1 | 2 | 3 | 4 | 5 | 6 | 7;
        ApiJobModel: {
            clusterId?: string | null;
            id?: string | null;
            jobDefinitionId?: string | null;
            triggerSourceType?: components["schemas"]["JobMasterTriggerSourceType"];
            bucketId?: string | null;
            agentConnectionId?: string | null;
            agentWorkerId?: string | null;
            hostId?: string | null;
            hostDisplayName?: string | null;
            priority?: components["schemas"]["JobMasterPriority"];
            /** Format: date-time */
            originalScheduledAt?: string;
            /** Format: date-time */
            scheduledAt?: string;
            msgData?: {
                [key: string]: unknown;
            } | null;
            metadata?: {
                [key: string]: unknown;
            } | null;
            status?: components["schemas"]["JobMasterJobStatus"];
            /** Format: int32 */
            numberOfFailures?: number;
            /** Format: date-span */
            timeout?: string;
            /** Format: int32 */
            maxNumberOfRetries?: number;
            /** Format: date-time */
            createdAt?: string;
            sourceId?: string | null;
            /** Format: date-time */
            processDeadline?: string | null;
            /** Format: date-time */
            processingStartedAt?: string | null;
            /** Format: date-time */
            succeedExecutedAt?: string | null;
            workerLane?: string | null;
        };
        /**
         * Format: int32
         * @enum {integer}
         */
        BucketStatus: 1 | 2 | 3 | 4 | 5 | 6;
        /**
         * Format: int32
         * @enum {integer}
         */
        ClusterMode: 1 | 2 | 3;
        /**
         * Format: int32
         * @enum {integer}
         */
        JobMasterJobStatus: 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8;
        /**
         * Format: int32
         * @enum {integer}
         */
        JobMasterPriority: 1 | 2 | 3 | 4 | 5;
        /**
         * Format: int32
         * @enum {integer}
         */
        JobMasterTriggerSourceType: 1 | 2 | 3;
        /**
         * Format: int32
         * @enum {integer}
         */
        RecurringScheduleStatus: 1 | 2 | 3 | 4 | 5;
        /**
         * Format: int32
         * @enum {integer}
         */
        RecurringScheduleType: 2 | 3;
    };
    responses: never;
    parameters: never;
    requestBodies: never;
    headers: never;
    pathItems: never;
}
export type $defs = Record<string, never>;
export type operations = Record<string, never>;
