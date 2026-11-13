# Plan: Hybrid Agent Connection (0.0.12 Alpha — Item 8)

Parent plan: [Plan0.0.12Alpha.md](Plan0.0.12Alpha.md) (Section 8).

---

## 1. Goal & Motivation

Allow a single agent connection to use **one provider for transport** (saving pending jobs, saving pending recurring schedules) and **another provider for execution** (dispatching jobs for processing, pulling/consuming jobs to execute, and draining processing buckets) — for example, **NATS JetStream** for high-throughput, low-latency save-pending ingestion and **RavenDB** or **Postgres** for durable execution queues (with no 5-minute `TransientThreshold` ceiling when NATS is only on the transport side).

Workers attach to a hybrid agent connection by name just like any other agent connection (`AddWorker("worker-1", "hybrid-conn-1")`).

---

## 2. Decided Groundwork

### 2.1 Shared Format for `RepositoryTypeId` and `Fingerprint`
Both strings are **always built by the framework** (never by a provider):
```
Format:      hybrid:<transport>&&<execution>
RepoType:    hybrid:NatsJetStream&&RavenDB
Fingerprint: hybrid:<nats-fp>&&<ravendb-fp>
```
- **Fixed role order** (`transport` first, `execution` second): swapping the transport and execution providers changes both `RepositoryTypeId` and `Fingerprint`, so `ProtectConnectionChanges` catches role swaps automatically. Changing either underlying database/broker endpoint also changes the composite fingerprint.
- **Separator `&&`**: reads as "both"; two characters so it won't appear by accident; unlike `+`, it does not silently decode to a space in URL query strings.
- **Same-provider hybrid allowed** (decided, Hugo 2026-11-01): e.g. `hybrid:Postgres&&Postgres` (even with the same connection string, which produces `hybrid:<fp>&&<fp>`).
  - For `NatsJetStream&&NatsJetStream` across two different NATS servers, `NatsJetStreamConnector.entries` will be keyed by `(config.Id, config.ConnectionString)` so the two legs don't collide on `config.Id` in the in-memory connection cache, while keeping `config.Id` (`{clusterId}:{name}`) for stream, subject, and KV names.
- **Reserved tokens (`hybrid:` prefix and `&&` separator)** enforced via hard validation (explicit throws):
  1. Any `IAgentFingerprintResolver` returning a fingerprint that contains `&&` or starts with `hybrid:` (case-insensitive) → throw `InvalidOperationException` (today all resolvers return GUIDs: SQL `ToString()`, NATS/RavenDB `ToString("N")`).
  2. Any provider `[JobMasterIocRegistration]` whose `RepositoryType` contains `&&` or `:` or starts with `hybrid:` → throw at registration (current IDs: `Postgres`, `MySql`, `SqlServer`, `NatsJetStream`, `RavenDB`).
  3. A hybrid connection whose transport or execution leg is itself hybrid (nested hybrid) → rejected at configuration/validation.

### 2.2 Storage Length Check (Verified)
- **Master**: `AgentConnectionRecord` (`RepositoryTypeId` and `Fingerprint`) and `BucketModel` (`RepositoryTypeId`) are stored as JSON in the master generic-record table (`MasterAgentConnectionService.SaveConnectionAsync`, `MasterBucketsService.CreateAsync`) — no column length limit.
- **Agent SQL**: `agent_conn_fingerprint.fingerprint` is `varchar(250)` ([AgentTableCreatorScripts.cs:88](src/StorageProviders/JobMaster.SqlBase/Scripts/AgentTableCreatorScripts.cs#L88)). Each underlying leg writes only its own GUID to its own store; the composite `hybrid:<transport-fp>&&<execution-fp>` (`~82` chars) is only ever saved on master.

### 2.3 API & Dashboard Encoding Check (Verified)
- Audited `JobMaster.Api` (`ApiAgentConnection.cs`, `AgentConnectionsEndpoints.cs`, `ApiBucketModel.cs`) and `JobMaster.Dashboard` (`agent-connections/+page.svelte`, `agent-connections/[id]/+page.svelte`):
  - Neither `RepositoryTypeId` nor `Fingerprint` is ever placed in a URL path or query parameter (routes use `agentConnection.Id.IdValue`, i.e., `{clusterId}:{name}`).
  - Both are serialized in JSON response bodies and rendered in Svelte via `{r.repositoryTypeId ?? "—"}`, which auto-escapes `&`.

---

## 3. Complete Codebase Inventory & Role Assignment

### 3.1 How Agent Repositories & Runners Split by Role
The core SDK's `AgentJobsDispatcherRepository` base ([AgentJobsDispatcherRepository.cs](src/JobMaster/Sdk/Repositories/AgentJobsDispatcherRepository.cs); built by the factory as `SingleAgentJobsDispatcherRepository` or `HybridJobsDispatcherRepository`) and `ClusterServiceKeys` ([ClusterServiceKeys.cs](src/JobMaster/Sdk/Ioc/ClusterServiceKeys.cs)) **already split agent operations into two `IAgentRawMessagesDispatcherRepository` instances**:
- `savePendingRepository` (`ClusterServiceKeys.GetAgentRawJobsDispatcherSavingKey(repoType)` = `"IAgentRawMessagesDispatcherRepository:SavingPending:{repoType}"`)
- `processingRepository` (`ClusterServiceKeys.GetAgentRawJobsDispatcherProcessingKey(repoType)` = `"IAgentRawMessagesDispatcherRepository:Processing:{repoType}"`)

Every method on `IAgentJobsDispatcherRepository` and every bucket-aware runner maps cleanly to **Transport**, **Execution**, or **Both**:

| Component / Method | Role | Bucket Address / Target |
|---|---|---|
| `PushForSaving` / `PushForSavingAsync` (Recurring) | **Transport** (`savePendingRepository`) | `{bucketId}:RecurSchedule-SavePending` |
| `PushSavePendingJob` / `PushSavePendingJobAsync` / `BulkPushSavePendingJobAsync` | **Transport** (`savePendingRepository`) | `{bucketId}:Job-SavePending` |
| `PullSavePendingJobsAsync` | **Transport** (`savePendingRepository`) | `{bucketId}:Job-SavePending` |
| `PullSavePendingRecurAsync` | **Transport** (`savePendingRepository`) | `{bucketId}:RecurSchedule-SavePending` |
| `IsPollingBasedForSaving` | **Transport** (`savePendingRepository.IsPollingBased`) | Controls whether polling or stream runner is used for saving |
| `ISavePendingJobsRunner` (`PollingSavePendingJobsRunner` / `NatsJetStreamSavePendingJobsRunner`) | **Transport** | Reads from `Job-SavePending`, writes to master / dispatches to processing |
| `ISaveRecurringSchedulerRunner` (`PollingSaveRecurringScheduleRunner` / `NatsJetStreamSaveRecurringScheduleRunner`) | **Transport** | Reads from `RecurSchedule-SavePending`, writes to master |
| `IDrainSavePendingJobsRunner` (`PollingDrainSavePendingJobsRunner` / `NatsJetStreamDrainSavePendingJobsRunner`) | **Transport** | Drains `Job-SavePending` to master |
| `IDrainSavePendingRecurringScheduleRunner` (`PollingDrainSavePendingRecurringScheduleRunner` / `NatsJetStreamDrainSavePendingRecurringScheduleRunner`) | **Transport** | Drains `RecurSchedule-SavePending` to master |
| `PushForProcessing` / `PushForProcessingAsync` / `BulkPushForProcessingAsync` | **Execution** (`processingRepository`) | `{bucketId}:Job-Processing` |
| `PullForProcessingAsync` | **Execution** (`processingRepository`) | `{bucketId}:Job-Processing` |
| `IsPollingBasedForProcessing` | **Execution** (`processingRepository.IsPollingBased`) | Controls whether polling or stream runner is used for execution |
| `IJobsExecutionRunner` (`PollingJobsExecutionRunner` / `NatsJetStreamJobsExecutionRunner`) | **Execution** | Reads from `Job-Processing`, runs jobs in `JobsExecutionEngine` |
| `IDrainProcessingJobsRunner` (`PollingDrainProcessingJobsRunner` / `NatsJetStreamDrainProcessingRunner`) | **Execution** | Drains `Job-Processing` back to master (`HeldOnMaster`) |
| `CreateBucketAsync(bucketId)` | **Both** | Creates `SavePending` + `RecurSchedule-SavePending` on **Transport**, and `Job-Processing` on **Execution** |
| `DestroyBucketAsync(bucketId)` | **Both** | Destroys `SavePending` + `RecurSchedule-SavePending` on **Transport**, and `Job-Processing` on **Execution** |
| `HasJobsAsync(bucketId)` | **Both** | Checks `SavePending` + `RecurSchedule-SavePending` on **Transport** OR `Job-Processing` on **Execution** |
| `IAgentFingerprintResolver` | **Both (Composite)** | Resolves `<transport-fp>` from **Transport** + `<execution-fp>` from **Execution** → `hybrid:<transport-fp>&&<execution-fp>` |

### 3.2 Inventory of Every Repo-Type-Keyed Call Site

| # | Call Site | File & Lines | Current Behavior | Hybrid Role / Required Change |
|---|---|---|---|---|
| 1 | `ClusterConfigBuilder.Finish` (IoC provider registration) | [ClusterConfigBuilder.cs:212-221](src/JobMaster/Sdk/Ioc/Setup/ClusterConfigBuilder.cs#L212-L221) | Collects `AgentConnections.Select(a => a.AgentRepoType)` and calls `RegisterProviderExtensionsForAgent`. | Expand any hybrid repo type via `HybridRepoType.TryParse` into its `transport` and `execution` repo types so `RegisterProviderExtensionsForAgent` is called for each underlying provider. |
| 2 | `JobMasterIocRegistrationAttribute.RegisterProviderExtensions` | [JobMasterIocRegistrationAttribute.cs:38-80](src/JobMaster/Sdk/Ioc/Setup/JobMasterIocRegistrationAttribute.cs#L38-L80) | Scans `[JobMasterIocRegistration]` classes and matches `RepositoryType`. | Validate that no provider `RepositoryType` contains `&&` or `:`. Never called with `"hybrid:..."` once `ClusterConfigBuilder` expands hybrid repo types into their two underlying provider types. |
| 3 | `ClusterConfigBuilder.ApplyJsonConfig` (`ConnectionOptionsBinderFactory.Create`) | [ClusterConfigBuilder.cs:553-560](src/JobMaster/Sdk/Ioc/Setup/ClusterConfigBuilder.cs#L553-L560), [ConnectionOptionsBinderFactory.cs:10-23](src/JobMaster/Sdk/Ioc/Setup/Binders/ConnectionOptionsBinderFactory.cs#L10-L23) | Binds `agent.ConnectionOptions` using `agent.RepositoryType`. | For a JSON hybrid connection (`Transport` + `Execution` sub-objects), call `ConnectionOptionsBinderFactory.Create(leg.RepositoryType)` on the transport leg selector and execution leg selector separately. |
| 4 | `AgentComponentFactory.GetRepository` & `AwareComponentFactory.GetRepositoryDispatcher` | [AgentComponentFactory.cs:59-78](src/JobMaster/Sdk/Repositories/AgentComponentFactory.cs#L59-L78), [JobMasterClusterAwareComponentFactory.cs:39-43](src/JobMaster/Sdk/Ioc/JobMasterClusterAwareComponentFactory.cs#L39-L43) | Looks up keyed `IAgentJobsDispatcherRepository` by `agentCnnConfig.RepositoryTypeId` and calls `repo.Initialize(agentCnnConfig)`. | When `HybridRepoType.TryParse(agentRepoTypeId)` succeeds, construct a `HybridJobsDispatcherRepository` resolving `SavingPending:{transportRepoType}` and `Processing:{executionRepoType}` from `ClusterServiceProvider`, and in `AgentJobsDispatcherRepository.Initialize` initialize `savePendingRepository` with `agentConnConfig.TransportConfig` and `processingRepository` with `agentConnConfig.ExecutionConfig`. |
| 5 | `AgentComponentFactory.GetFingerprintResolver` & `AwareComponentFactory.GetFingerprintResolver` | [AgentComponentFactory.cs:38-57](src/JobMaster/Sdk/Repositories/AgentComponentFactory.cs#L38-L57), [JobMasterClusterAwareComponentFactory.cs:45-49](src/JobMaster/Sdk/Ioc/JobMasterClusterAwareComponentFactory.cs#L45-L49) | Looks up keyed `IAgentFingerprintResolver` by `config.RepositoryTypeId` and calls `repo.Initialize(config)`. | When `HybridRepoType.TryParse(agentRepoTypeId)` succeeds, construct a `HybridAgentFingerprintResolver` wrapping the transport resolver (initialized with `config.TransportConfig`) and execution resolver (initialized with `config.ExecutionConfig`). Also validate single-provider fingerprints don't contain `&&` or start with `hybrid:`. |
| 6 | `BucketRunnersFactory` — Save-pending & Drain-save-pending runners | [BucketRunnersFactory.cs:39-58, 81-100, 132-170](src/JobMaster/Sdk/Background/Runners/BucketRunnersFactory.cs#L39-L58) | When `!IsPollingBasedForSaving`, resolves `GetBucketAwareRunner<TRunner>(agentCnnConfig.RepositoryTypeId, worker)`. | **Transport role**: pass `agentCnnConfig.TransportConfig.RepositoryTypeId` for `IDrainSavePendingJobsRunner`, `IDrainSavePendingRecurringScheduleRunner`, `ISavePendingJobsRunner`, and `ISaveRecurringSchedulerRunner`. |
| 7 | `BucketRunnersFactory` — Execution & Drain-processing runners | [BucketRunnersFactory.cs:60-79, 102-130](src/JobMaster/Sdk/Background/Runners/BucketRunnersFactory.cs#L60-L79) | When `!IsPollingBasedForProcessing`, resolves `GetBucketAwareRunner<TRunner>(agentCnnConfig.RepositoryTypeId, worker)`. | **Execution role**: pass `agentCnnConfig.ExecutionConfig.RepositoryTypeId` for `IDrainProcessingJobsRunner` and `IJobsExecutionRunner`. |
| 8 | `NatsJetStreamRunnerBase.DefineBucketId` | [NatsJetStreamRunnerBase.cs:274-280](src/StorageProviders/JobMaster.NatsJetStream/Background/NatsJetStreamRunnerBase.cs#L274-L280) | Reads `agentConnectionConfig = BackgroundAgentWorker.ClusterConnConfig.GetAgentConnectionConfig(agentConnectionId.IdValue)`. | **Per-runner role**: NATS save-pending / drain-save-pending runners resolve `agentConnectionConfig.TransportConfig`, while NATS execution / drain-processing runners resolve `agentConnectionConfig.ExecutionConfig` so `agentConnectionConfig.ConnectionString` and `AdditionalConnConfig` (auth/TLS) are the NATS leg's config. |
| 9 | `SqlJobMasterRuntimeSetup.ConfigAgentsAsync` | [SqlJobMasterRuntimeSetup.cs:141-181](src/StorageProviders/JobMaster.SqlBase/SqlJobMasterRuntimeSetup.cs#L141-L181) | Queries `GetAllAgentConnectionConfigs().Where(a => a.RepositoryTypeId == RepositoryTypeId)` and provisions SQL agent tables using `SqlGeneratorFactory.Get(agentConfig.RepositoryTypeId)`. | **Both legs (if SQL)**: iterate over `GetAllConcreteAgentConnectionConfigs()`, which yields `TransportConfig` and `ExecutionConfig` for hybrid connections (or the config itself for non-hybrid). Each SQL leg has its own concrete `RepositoryTypeId`, `ConnectionString`, and `AdditionalConnConfig` (table prefix). |
| 10 | `PostgresJobMasterRuntimeSetup.CreateCaseInsensitiveCollationAsync` | [PostgresJobMasterRuntimeSetup.cs:53-74](src/StorageProviders/JobMaster.Postgres/PostgresJobMasterRuntimeSetup.cs#L53-L74) | Queries `GetAllAgentConnectionConfigs().Where(a => a.RepositoryTypeId == RepositoryTypeId)` to create collation on agent DBs. | **Both legs (if Postgres)**: use `GetAllConcreteAgentConnectionConfigs()` so a Postgres transport or execution leg gets the collation created. |
| 11 | `RavenDbJobMasterRuntimeSetup.OnBeforeStartAsync` | [RavenDbJobMasterRuntimeSetup.cs:49-66](src/StorageProviders/JobMaster.RavenDB/RavenDbJobMasterRuntimeSetup.cs#L49-L66) | Queries `GetAllAgentConnectionConfigs().Where(a => a.RepositoryTypeId == RavenDbRepositoryConstants.RepositoryTypeId)` to deploy `RavenDbMessageIndexes`. | **Both legs (if RavenDB)**: use `GetAllConcreteAgentConnectionConfigs()` so a RavenDB transport or execution leg gets `RavenDbMessageIndexes` deployed on its database/collection prefix. |
| 12 | `NatsJetStreamJobMasterRuntimeSetup.ValidateAsync` | [NatsJetStreamJobMasterRuntimeSetup.cs:16-45](src/StorageProviders/JobMaster.NatsJetStream/NatsJetStreamJobMasterRuntimeSetup.cs#L16-L45) | Queries `GetAllAgentConnectionConfigs().Where(a => a.RepositoryTypeId == NatsJetStreamConstants.RepositoryTypeId)` and enforces `TransientThreshold <= 5 min`. | **Execution leg only**: filter by `a.ExecutionConfig.RepositoryTypeId == NatsJetStreamConstants.RepositoryTypeId`. Also cap the `transientThreshold` passed to `NatsJetStreamConstants.CalcMaxAckPending` at `NatsJetStreamConstants.MaxThreshold` (5 min) in `NatsJetStreamRunnerBase` / `NatsJetStreamConnector`. |
| 13 | `SqlRawMessagesDispatcherRepositoryBase.Initialize` & `SqlAgentFingerprintResolver.Initialize` | [SqlRawMessagesDispatcherRepositoryBase.cs:41-46](src/StorageProviders/JobMaster.SqlBase/Agents/SqlRawMessagesDispatcherRepositoryBase.cs#L41-L46), [SqlAgentFingerprintResolver.cs:71-76](src/StorageProviders/JobMaster.SqlBase/Agents/SqlAgentFingerprintResolver.cs#L71-L76) | Calls `SqlGeneratorFactory.Get(this.AgentRepoTypeId)`. | **Already safe!** `this.AgentRepoTypeId` is the concrete subclass property (`"Postgres"`, `"MySql"`, `"SqlServer"`), not `config.RepositoryTypeId`. |
| 14 | `KnownExceptionIdentifier` | [KnownExceptionIdentifier.cs:12-30](src/JobMaster/Sdk/Utils/KnownExceptionIdentifier.cs#L12-L30) | Keyed by `s.RepoType` (`"Postgres"`, `"MySql"`, `"SqlServer"`). | **Already safe!** Only called on Master (`ClusterConnConfig.RepositoryTypeId`) and inside `SqlAgentFingerprintResolver` which passes its own concrete `this.AgentRepoTypeId`. |
| 15 | `OperationThrottlerSettingsFactory` | [OperationThrottlerSettingsFactory.cs:14-90](src/JobMaster/Sdk/Abstractions/Repositories/OperationThrottlerSettingsFactory.cs#L14-L90) | Resolves throttlers and `MaxBatchSize` by `AgentRepositoryTypeId(agentConnectionId)`. | **Split by role** (`Transport` vs `Execution`) — see Section 4.4 below. |

---

## 4. Detailed Design (Decided, Hugo 2026-11-01)

### 4.1 Fluent & JSON Configuration API

#### Selector Interfaces
`AgentConnName` and `ProtectConnectionChanges` belong only on the top-level connection selector (not inside `.Transport(...)` or `.Execution(...)`):
```csharp
public interface IAgentConnectionProviderSelector
{
    internal void AgentAdditionalConnConfig(JobMasterConfigDictionary additionalConnConfig);
    internal void AppendAdditionalConnConfigValue(JobMasterNamespaceUniqueKey namespaceKey, string key, object value);
    internal void AgentRepoType(string repoType);
    internal void AgentConnString(string connString);
}

public interface IAgentConnectionConfigSelector : IAgentConnectionProviderSelector
{
    IAgentConnectionConfigSelector AgentConnName(string agentConnName);
    IAgentConnectionConfigSelector ProtectConnectionChanges(bool protectConnectionChanges);
    IHybridAgentConnectionConfigSelector UseHybrid();
}

public interface IHybridAgentConnectionConfigSelector
{
    IHybridAgentConnectionConfigSelector AgentConnName(string agentConnName);
    IHybridAgentConnectionConfigSelector ProtectConnectionChanges(bool protectConnectionChanges);
    IHybridAgentConnectionConfigSelector Transport(Action<IAgentConnectionProviderSelector> configure);
    IHybridAgentConnectionConfigSelector Execution(Action<IAgentConnectionProviderSelector> configure);
}
```
And in each provider's `ConfigExtensions` (`Postgres`, `MySql`, `SqlServer`, `RavenDB`, `NatsJetStream`):
```csharp
public static T UsePostgresForAgent<T>(this T agentConfigSelector, string connectionString, string? tablePrefix = null)
    where T : IAgentConnectionProviderSelector
```
(and `IConnectionOptionsBinder.SetOptions(IAgentConnectionProviderSelector selector, IDictionary<string, object> options)`).

With this:
- Standard connections keep chaining `.UsePostgresForAgent(cs).ProtectConnectionChanges(false)` because `T` is `IAgentConnectionConfigSelector`.
- Hybrid connections read naturally and only expose provider configuration inside `.Transport(...)` and `.Execution(...)`:
  ```csharp
  c.AddAgentConnectionConfig("hybrid-connection-1")
      .UseHybrid()
      .Transport(t => t.UseNatsJetStream("nats://localhost:4222"))
      .Execution(e => e.UseRavenDb("Urls=http://localhost:8080;Database=Exec"))
      .ProtectConnectionChanges(true);
  ```

#### JSON Config (`ConfigFromJson`)
In `AgentConnectionJsonConfig`:
```json
{
  "Name": "hybrid-connection-1",
  "ProtectConnectionChanges": true,
  "Transport": {
    "RepositoryType": "NatsJetStream",
    "ConnectionString": "nats://localhost:4222",
    "ConnectionOptions": { ... }
  },
  "Execution": {
    "RepositoryType": "RavenDB",
    "ConnectionString": "Urls=http://localhost:8080;Database=Exec",
    "ConnectionOptions": { ... }
  }
}
```
When `Transport` or `Execution` is present in JSON, it configures the hybrid legs and binds each leg's `ConnectionOptions` via `ConnectionOptionsBinderFactory.Create(leg.RepositoryType)`. Mixing top-level `RepositoryType`/`ConnectionString`/`ConnectionOptions` with `Transport`/`Execution` on the same entry is rejected.

### 4.2 Config Model Representation (`AgentConnectionDefinition` & `JobMasterAgentConnectionConfig`)

1. **`HybridRepoType` helper** (`src/JobMaster/Sdk/Utils/HybridRepoType.cs`):
   - `public const string Prefix = "hybrid:";`
   - `public const string Separator = "&&";`
   - `public static string Format(string transportRepoType, string executionRepoType)`
   - `public static bool IsHybrid(string? repoType)`
   - `public static bool TryParse(string? repoType, out string transportRepoType, out string executionRepoType)`
   - `public static void ValidateProviderRepoTypeId(string repoType)` (rejects `:`, `&&`, `hybrid:`)
   - `public static void ValidateSingleFingerprint(string fingerprint)` (rejects `&&`, `hybrid:`)
   - `public static string FormatFingerprint(string transportFingerprint, string executionFingerprint)`

2. **`AgentConnectionDefinition`**:
   - Keeps `AgentConnectionName`, `ClusterId`, `ProtectConnectionChanges`.
   - Adds `bool IsHybrid { get; set; }`, `AgentConnectionTargetDefinition? Transport { get; set; }`, `AgentConnectionTargetDefinition? Execution { get; set; }`.
   - When `IsHybrid` is true and both legs have `AgentRepoType` set, `AgentRepoType` returns `HybridRepoType.Format(Transport.AgentRepoType, Execution.AgentRepoType)`.

3. **`JobMasterAgentConnectionConfig`**:
   - For a standard (non-hybrid) connection:
     - `IsHybrid => false`
     - `TransportConfig => this`
     - `ExecutionConfig => this`
   - For a hybrid connection:
     - `IsHybrid => true`
     - `RepositoryTypeId` = `"hybrid:<transportRepoType>&&<executionRepoType>"`
     - `ConnectionString` = `string.Empty`
     - `TransportConfig` = `new JobMasterAgentConnectionConfig(clusterId, name, transportConnString, transportRepoTypeId, transportAdditionalConnConfig)`
     - `ExecutionConfig` = `new JobMasterAgentConnectionConfig(clusterId, name, executionConnString, executionRepoTypeId, executionAdditionalConnConfig)`
   - `JobMasterClusterConnectionConfig.GetAllConcreteAgentConnectionConfigs()`:
     - For a non-hybrid connection, yields `[conn]`.
     - For a hybrid connection, yields `[conn.TransportConfig, conn.ExecutionConfig]`.

### 4.3 NATS `TransientThreshold` & Consumer `MaxAckPending`
- `NatsJetStreamJobMasterRuntimeSetup.ValidateAsync` checks `a.ExecutionConfig.RepositoryTypeId == NatsJetStreamConstants.RepositoryTypeId`. When NATS is only the **Transport** provider, `TransientThreshold` can exceed 5 minutes.
- In `NatsJetStreamRunnerBase.OnTickAsync` (or `NatsJetStreamConnector.CreateOrUpdateConsumerInternalAsync`), the `transientThreshold` passed to `NatsJetStreamConstants.CalcMaxAckPending` is capped at `NatsJetStreamConstants.MaxThreshold` (5 min) so a large cluster `TransientThreshold` on a NATS-transport hybrid connection does not inflate `MaxAckPending`.

### 4.4 Throttlers & Batch Sizes Per Role (`OperationThrottlerSettingsFactory`)
- **`GetSchedulingAgentThrottler(string agentConnectionId)`**: uses `TransportConfig.RepositoryTypeId` (since scheduling pushes `SavePending` items to the Transport store).
- **Internal Agent Throttlers**:
  - `GetInternalAgentTransportThrottler(string agentConnectionId)`: uses `TransportConfig.RepositoryTypeId`. Used by `AgentJobsDispatcherService` for `AddSavePendingJob*`, `BulkAddSavePendingJobAsync`, `AddSavePendingRecur*`, `PullSavePendingJobsAsync`, `PullSavePendingRecurAsync`.
  - `GetInternalAgentExecutionThrottler(string agentConnectionId)`: uses `ExecutionConfig.RepositoryTypeId`. Used by `AgentJobsDispatcherService` for `AddForProcessingAsync`, `BulkAddForProcessingAsync`, `PullForProcessingAsync`.
  - For non-hybrid connections, both return the same shared throttler instance from `InternalAgentThrottlers[agentConnectionId]` so single-store connections keep one shared semaphore. For hybrid connections, Transport and Execution get separate semaphores sized for their respective providers (`"{agentConnectionId}:transport"` and `"{agentConnectionId}:execution"`).
- **Max Batch Size**:
  - `GetAgentTransportMaxBatchSize(string agentConnectionId)` → uses `TransportConfig.RepositoryTypeId` (used by `JobMasterSchedulerClusterAware.BulkScheduleAsync`).
  - `GetAgentExecutionMaxBatchSize(string agentConnectionId)` → uses `ExecutionConfig.RepositoryTypeId` (used by `AssignJobsToBucketsRunner.DispatchJobsToBucketsAsync`).

---

## 5. Implementation Tasks

- [x] **1. `HybridRepoType` & Reserved Token Guards**
  - Create `HybridRepoType` helper (`Format`, `TryParse`, `IsHybrid`, `FormatFingerprint`, `ValidateProviderRepoTypeId`, `ValidateSingleFingerprint`).
  - Guard provider registration in `JobMasterIocRegistrationAttribute` against `RepositoryType` containing `:` or `&&`.
  - Guard `IAgentFingerprintResolver` output against `&&` or `hybrid:` prefix.
- [x] **2. Config Models (`AgentConnectionDefinition`, `JobMasterAgentConnectionConfig`, `JobMasterClusterConnectionConfig`)**
  - Add `Transport` and `Execution` leg definitions to `AgentConnectionDefinition`.
  - Add `TransportConfig`, `ExecutionConfig`, and `IsHybrid` to `JobMasterAgentConnectionConfig`.
  - Add `AddHybridAgentConnectionConfig` and `GetAllConcreteAgentConnectionConfigs()` to `JobMasterClusterConnectionConfig`.
- [x] **3. Fluent & JSON Configuration**
  - Extract `IAgentConnectionProviderSelector`, add `IHybridAgentConnectionConfigSelector` and `.UseHybrid()` on `IAgentConnectionConfigSelector`.
  - Update provider `ConfigExtensions` (`Postgres`, `MySql`, `SqlServer`, `RavenDB`, `NatsJetStream`) and `IConnectionOptionsBinder` to accept `IAgentConnectionProviderSelector`.
  - Add `Transport` and `Execution` sub-configs to `AgentConnectionJsonConfig` and wire into `ClusterConfigBuilder.ApplyJsonConfig`.
- [x] **4. IoC, Dispatcher, Fingerprint, Runners & Throttlers**
  - `ClusterConfigBuilder.Finish`: expand hybrid agent repo types so both underlying providers' `RegisterProviderExtensionsForAgent` run.
  - Implement `HybridJobsDispatcherRepository` and `HybridAgentFingerprintResolver` and wire into `JobMasterClusterAwareComponentFactory` / `AgentComponentFactory` (plus `AgentJobsDispatcherRepository.Initialize` passing `TransportConfig` and `ExecutionConfig`).
  - Update `BucketRunnersFactory` to pass `TransportConfig.RepositoryTypeId` for save-pending/drain-save-pending runners and `ExecutionConfig.RepositoryTypeId` for execution/drain-processing runners.
  - Update `NatsJetStreamRunnerBase` so NATS runners resolve `TransportConfig` vs `ExecutionConfig` according to the runner's role, cap `TransientThreshold` at `NatsJetStreamConstants.MaxThreshold` for `CreateOrUpdateConsumerAsync`, and key `NatsJetStreamConnector.entries` by `(config.Id, config.ConnectionString)`.
  - Update `OperationThrottlerSettingsFactory` and `AgentJobsDispatcherService` / `JobMasterSchedulerClusterAware` / `AssignJobsToBucketsRunner` to use role-specific throttlers and batch sizes.
- [x] **5. Provisioning & Startup Validation**
  - Update `SqlJobMasterRuntimeSetup`, `PostgresJobMasterRuntimeSetup`, and `RavenDbJobMasterRuntimeSetup` to iterate `GetAllConcreteAgentConnectionConfigs()`.
  - Update `NatsJetStreamJobMasterRuntimeSetup.ValidateAsync` to check `TransientThreshold` only when NATS is the **Execution** provider (`a.ExecutionConfig.RepositoryTypeId == NatsJetStreamConstants.RepositoryTypeId`).
  - Update `JobMasterRuntime.ValidateClusterDefinitions` and `ClusterConfigBuilder` for hybrid connection validation (missing leg, missing connString/repoType on either leg, nested hybrid, mixing single-provider and hybrid on the same connection).
- [x] **6. Tests & ChangeLog**
  - Unit tests: `HybridRepoTypeTests`, fluent + JSON config tests, validation failure tests (missing leg, NATS execution vs transport `TransientThreshold`, fingerprint protection on role swap / leg change), throttler role resolution tests, dispatcher & runner factory resolution tests.
  - Update `Plan0.0.12Alpha.md` and `ChangeLog`.

## 6. Review Follow-ups (Next Session)

- [ ] Remove `ValidatedSingleAgentFingerprintResolver` wrapper from `JobMasterClusterAwareComponentFactory.GetFingerprintResolver`; validate single fingerprints directly in `JobMasterRuntime.ValidateFingerprintAsync` (and inside `HybridRepoType.FormatFingerprint`).
- [x] Add transient lifetime / single-initialization guards for agent dispatchers and fingerprint resolvers (`ClusterIocRegistration.BuildFactory` and `AgentJobsDispatcherRepository.Initialize`).
      Done: transient-lifetime check; raw messages repos (`IsInitialized`, second `Initialize` throws; the dispatcher base
      skips already-initialized repos, so Single's shared repo is initialized once). Same for fingerprint resolvers (`IAgentRawFingerprintResolver.IsInitialized`).
- [ ] Add hybrid agent connection examples and architecture guidance to `jobmaster-doc` (with Item 10/11).
- [x] Dispatcher: non-generic `AgentJobsDispatcherRepository` base + sealed `SingleAgentJobsDispatcherRepository` /
      `HybridJobsDispatcherRepository`, both built with `new` in the factory; provider dispatcher subclasses removed (Hugo, 2026-11-04).
- [x] Fingerprint resolver: same shape — `IAgentRawFingerprintResolver` (providers `<P>AgentRawFingerprintResolver`), abstract `AgentFingerprintResolver`, sealed `SingleAgentFingerprintResolver` / `HybridAgentFingerprintResolver` (Hugo, 2026-11-04).
