# Plan: future features (post-0.0.12)

Backlog of features not tied to a release yet. Same layout as [Plan0.0.12Alpha.md](Plan0.0.12Alpha.md): goal, current
state, design, open decisions (settle before coding), tasks, and done criteria. One PR per item.

| #  | Item                                                  | Size  | Breaking | Depends on |
|----|-------------------------------------------------------|-------|----------|------------|
| F1 | OpenTelemetry logging (forward to `ILogger`)          | M     | No (log enums become public) | – (aligns with 0.0.12 item 17) |
| F2 | OpenTelemetry tracing                                 | L     | – (not designed) | F1 |
| F3 | Dashboard alerts                                      | L     | – (not designed) | – (independent of F1/F2) |
| F4 | AI debug (Dashboard chat)                             | L     | – (not designed) | F5 (chat history); reuses `JobMaster.Api` |
| F5 | Generic storage API (custom groups)                   | M     | No       | – (needed by F3 rules/state, F4 chats/settings) |
| X1 | Fix: Dashboard OpenAPI seeder trusts the `Host` header | XS   | No       | – (bug, do before F4/F5) |

---

# OpenTelemetry

**Scope.** JobMaster only maps its own concepts onto OpenTelemetry concepts: which signals it emits (logs, traces,
later metrics), the category / source / meter names, the `jobmaster.*` attributes, and the spans. What users do with
that data (backends, dashboards, alert rules) is up to them. JobMaster ships no alert rules, Grafana dashboards or
"mixins". Built-in alerting is a separate feature, see [Dashboard alerts](#dashboard-alerts).

**Names are public API.** Log categories, `jobmaster.*` attributes and future span/metric names become something users
filter and alert on. Renaming one silently breaks their queries, so renames get a ChangeLog entry and a deprecation
period, like method renames.

## F1. OpenTelemetry logging

**Goal.** JobMaster's own logs (runners, execution engine, providers, API) reach the user's OpenTelemetry pipeline
(OTLP → Grafana/Loki, Seq, Datadog, Azure Monitor, …) as structured log records, with per-category and per-level
control. Logs a user writes inside a handler carry the job's ids, so they can be filtered by job.

**Current state.**
- All SDK logging goes through the internal `IJobMasterLogger` ([JobMasterLogger.cs](src/JobMaster/Sdk/Services/Master/JobMasterLogger.cs)),
  ~220 call sites across core + NATS/RavenDB/SqlBase + `JobMaster.Api`, via the `Debug/Info/Warn/Error/Critical`
  extensions ([JobMasterLoggerExtensions.cs](src/JobMaster/Sdk/Abstractions/Extensions/JobMasterLoggerExtensions.cs)).
- Only two outputs:
  - **Master DB**: buffered, flushed every 15s / 100 items. `Debug` is dropped, and when the queue holds 10k items new
    logs are dropped too (e.g. master DB down). This is what the Dashboard log page reads.
  - **Mirror**: `ClusterConnectionConfig.MirrorLog` (`Action<LogItem>`), used only by `DebugJsonlFileLogger`. It gets
    every item, including `Debug` and the ones dropped when the queue is full or the logger is disposed.
- No `Microsoft.Extensions.Logging` anywhere in `src`. `JobMaster.csproj` references only DI + Configuration
  abstractions. No `ActivitySource` / `Meter` either.
- `JobMasterLogCategory` (`Job`, `JobExecution`, `AgentWorker`, `Bucket`, `Cluster`, `RecurringSchedule`, `Api`) and
  `JobMasterLogLevel` are `internal` ([JobMasterLogLevel.cs](src/JobMaster/Sdk/Abstractions/Models/Logs/JobMasterLogLevel.cs)).
  `category` is optional on `Log`, so some calls have none.
- Things that matter for a bridge:
  - **Mirror is a single slot.** `DebugJsonlFileLogger` sets `MirrorLog = JsonlFileLogger.LogMirror`, and the internal
    `EnableMirrorLog` overwrites it. A second sink would replace the JSONL one. `JobMasterRuntime` checks
    `MirrorLog == JsonlFileLogger.LogMirror` to set up the file logger.
  - **Exception is flattened.** `Log` appends `exception.ToString()` to `Message` *before* calling the mirror, and
    `LogItem` has no `Exception`. An `ILogger` sink must get the original `Exception` so exporters fill
    `exception.type` / `exception.message` / `exception.stacktrace`.
  - **Separate container per cluster.** `JobMasterLogger` is a singleton in `ClusterIocRegistration.ClusterServices`,
    built inside `AddJobMasterCluster`, before the app's `IServiceProvider` exists. It can't take `ILoggerFactory`
    through its constructor. The app provider is reachable at runtime (`backgroundAgentWorker.ServiceProvider` is
    what creates the handler scope in `JobsExecutionEngine`).
  - Messages are interpolated strings (`$"JobId={job.Id}"`), not message templates. Structured fields today are only
    `LogItem`'s: `ClusterId`, `Level`, `Category`, `ReferenceId` (`"N"` Guid), `Host`, `SourceMember/File/Line`.
  - Some categories are inaccurate for filtering, e.g. the `ExecuteJobAsync completed` line in `JobsExecutionEngine`
    is tagged `Job`, not `JobExecution`.
  - Errors in the logger itself (`[JM-LOGGER] Flush failed`, …) go to `Trace.TraceError` only.
- Handlers are resolved from a per-execution DI scope (`JobsExecutionEngine`, `CreateAsyncScope` → `HandleAsync`).
  Users can already inject `ILogger<T>` there, but no logging scope is opened, so their logs have no job id.

**Design (proposed).**
- **Bridge to `Microsoft.Extensions.Logging`, not to the OpenTelemetry SDK.** In .NET the OTel logs API *is*
  `ILogger`: users already call `builder.Logging.AddOpenTelemetry(...)`. Forwarding to `ILogger` covers OTel and every
  other `ILogger` provider (console, Serilog, Seq) without JobMaster taking a dependency on OpenTelemetry.
  New dependency: `Microsoft.Extensions.Logging.Abstractions` (supports netstandard2.0, same family as the DI/Config
  abstractions already referenced).
- **Opt-in per cluster**, next to `DebugJsonlFileLogger`, with JobMaster-side category and level filters:
  ```csharp
  services.AddJobMasterCluster("cluster-1", c => c
      .UsePostgresForMaster(...)
      .ForwardLogsToILogger(o =>
      {
          o.Categories = [JobMasterLogCategory.JobExecution];   // default: null = all categories
          o.MinimumLevel = JobMasterLogLevel.Info;               // default: Info
      }));
  ```
  - Parameterless `.ForwardLogsToILogger()` = all categories, `Info`.
  - Options class `JobMasterLogForwardingOptions` (name to confirm), stored on `ClusterDefinition` like the
    `MirrorLog*` fields.
  - Two filter layers: the bridge checks the JobMaster filter first (category in set, level ≥ minimum), then
    `logger.IsEnabled` applies as usual, so the user's `Logging` config can narrow it further.
  - `MinimumLevel = Debug` forwards `Debug`, even though the master DB still drops it.
  - Hard validation: an empty `Categories` list → throw at configuration ("forwards nothing"; don't enable instead).
  - JSON config key in `ClusterJsonConfig`: `"ForwardLogsToILogger": { "Categories": ["JobExecution"], "MinimumLevel": "Info" }`,
    enum names parsed ignoring case (same convention as cluster ids).
- **Public API change.** `JobMasterLogCategory` and `JobMasterLogLevel` become public, with XML docs per value in user
  language (what kind of logs it covers). Adding or renaming a category is a public change from then on.
- **Sink list instead of the single mirror slot.** An internal `ILogSink { void Write(LogItem item, Exception? ex); }`
  list on the cluster connection config. JSONL becomes one sink, the `ILogger` bridge another. `JobMasterLogger.Log`
  calls the sinks with the raw exception before flattening it into `Message` for the DB. The JSONL sink keeps
  today's output (flattens the exception itself). Replaces the `MirrorLog == JsonlFileLogger.LogMirror` check in
  `JobMasterRuntime`.
- **Late-bound `ILoggerFactory`.** At runtime start, resolve `ILoggerFactory` from the app provider and hand it to the
  cluster's bridge sink. Until then the sink does nothing. `ForwardLogsToILogger()` with no `ILoggerFactory` in the
  app → startup error (hard rule; the user asked for it).
- **Mapping.**
  - Logger category: `JobMaster.<JobMasterLogCategory>` (e.g. `JobMaster.JobExecution`, `JobMaster.Api`), `JobMaster`
    when there's no category. Useful for per-provider filtering in the user's `Logging` config.
  - Level: `Debug→Debug`, `Info→Information`, `Warning→Warning`, `Error→Error`, `Critical→Critical`. Check
    `logger.IsEnabled(level)` first; the bridge does nothing more for disabled levels.
  - Body: the message as is. Attributes (state as `IReadOnlyList<KeyValuePair<string, object?>>`, which OTel turns
    into log attributes): `jobmaster.cluster_id`, `jobmaster.category`, `jobmaster.reference_id`, `jobmaster.log_id`,
    the OTel semantic-convention code attributes from `SourceMember/File/Line`, and `host.name` from `LogItem.Host`.
  - `host.name` as a record attribute because OTel's default resource has none: it's only there when the app adds a
    host detector or `OTEL_RESOURCE_ATTRIBUTES`. If the app sets one, both carry the same value.
  - Exception: passed as `ILogger`'s `exception` argument, not in the body.
- **Independent of the DB.** The sink runs before the queue-full / disposed / `Debug` checks, like the mirror today, so
  the OTel output keeps working while master is down: that's when it's most needed.
- **Handler logging scope.** When an `ILoggerFactory` is available, wrap `HandleAsync` in `BeginScope` with the job's
  ids (`jobmaster.job_id`, `jobmaster.cluster_id`, `jobmaster.job_definition_id`, `jobmaster.recurring_schedule_id`
  if any, `jobmaster.attempt`). With `IncludeScopes = true` in the OTel logging options, every user log inside the
  handler carries them. Cheap: one scope per execution.
- **Logger self-errors.** `[JM-LOGGER]` flush/insert failures also go to the bridge under `JobMaster.Logger`, written
  straight to `ILogger` (never through `JobMasterLogger.Log`, to avoid recursion).

**Possible attributes (not decided, added step by step).**
- Rule: the OTel **resource** describes the whole process and belongs to the app; JobMaster never sets it. Every
  JobMaster concept can vary inside one process, so they're all **record attributes**. E.g. a JobMaster host is
  cluster + machine (`MasterHostService.RegisterNewHostAsync`), so a process on 2 clusters has 2 hosts.
- JobMaster concepts available, as a chain: cluster → host → agent connection → agent worker → bucket → job /
  recurring schedule / execution. Candidate names: `jobmaster.cluster_id`, `jobmaster.host_id`,
  `jobmaster.agent_connection_id`, `jobmaster.agent_worker_id`, `jobmaster.bucket_id`, `jobmaster.job_id`, …
  Each one set only where it's known (e.g. no bucket on coordinator runner logs, no worker on scheduler calls from
  app code, `JobMaster.Api` or bootstrap).
- Worker / connection / bucket ids aren't on the logger today: `JobMasterLogger` is one per cluster, and one process
  can run several workers on a cluster. The ids live on the worker (`JobMasterBackgroundAgentWorker.AgentWorkerId`,
  `agentWorkerSnapshot.AgentConnectionId`, null for a coordinator-only worker). Likely mechanism: an internal ambient
  context (`AsyncLocal`, e.g. `JobMasterLogContext`) set where work starts (runner loop in `JobMasterRunner`,
  execution path in `JobsExecutionEngine`), read by the bridge and by the handler scope. No change to the call sites.
- Store them on `LogItem` for the Dashboard too (schema change), or bridge-only?

**Open decisions.**
- Uncategorized logs (`category: null`) when `Categories` is set: drop them, or always forward? Or remove the case:
  the category audit gives every call site a category and `Log` requires one.
- Move the two enums to a public-facing namespace when making them public, or keep
  `JobMaster.Sdk.Abstractions.Models.Logs`?
- Option names: `ForwardLogsToILogger`, `JobMasterLogForwardingOptions`, `Categories`, `MinimumLevel`.
- Per cluster (like `DebugJsonlFileLogger`) or once globally? Per cluster matches the existing API; most apps would
  turn it on for all clusters.
- Should the handler scope be on by default (whenever `ILoggerFactory` exists) or only with the opt-in?
- Id format in attributes: `"N"` (as stored) or base64 (as the dashboard shows, 0.0.12 item 17). Base64 makes
  "copy id from dashboard → search in the backend" work; settle together with item 17.
- An option to stop writing logs to master (`StoreLogsOnMaster(false)`) for users who only want OTel: saves DB
  load, but the Dashboard log page goes empty. Separate item?
- `Debug` messages are interpolated at the call site even when nothing reads them (existing cost, more visible now
  that `Debug` can be forwarded). An interpolated-string handler or `IsEnabled` pre-check: in or out of scope?

**Tasks.**
- [ ] Make `JobMasterLogCategory` / `JobMasterLogLevel` public + XML docs.
- [ ] Category audit of the ~220 `IJobMasterLogger` call sites, so per-category forwarding is accurate (execution logs
      tagged `JobExecution`, etc.).
- [ ] `ILogSink` list on the cluster connection config; move JSONL onto it; `JobMasterLogger.Log` calls the sinks with
      the raw exception. Existing JSONL output unchanged (unit test).
- [ ] `ILogger` bridge sink (filters, category, level, attributes, exception) + late-bound `ILoggerFactory` at runtime
      start.
- [ ] Options class + fluent overloads (with / without configure) + JSON config key; empty-list validation; startup
      error when enabled without `ILoggerFactory`.
- [ ] Handler `BeginScope` in `JobsExecutionEngine`.
- [ ] `[JM-LOGGER]` self-errors to the bridge.
- [ ] Unit tests (with a capturing `ILoggerProvider`): default forwards all categories at `Info+` and drops `Debug`; a
      category set forwards only those; `MinimumLevel` and `IsEnabled` both respected; uncategorized behavior per
      decision; empty list throws; JSON binding; attributes present (incl. `host.name`); exception passed separately,
      not in the body; sink still called when the DB queue is full; JSONL + bridge both receive the same item; handler
      logs carry the scope ids.
- [ ] Sample: `samples/JobMaster.SampleWeb` with `AddOpenTelemetry()` logging + OTLP exporter; check that the records
      arrive with the attributes, e.g. a local OTel Collector / Aspire dashboard container. Verify, don't assume.
- [ ] Docs (new monitoring page: setup, options + defaults with a `JobExecution`-only example, the note that `ILogger`
      filters still apply on top, category names, attribute list, `IncludeScopes`) + ChangeLog (added + enums public).

**Done when.** With the opt-in on, JobMaster logs (filtered by the configured categories and level) and handler logs
show up in an OTLP backend with the `jobmaster.*` attributes and real exception fields. With it off, nothing changes.
Unit suite green.

## F2. OpenTelemetry tracing

**Status: idea registered, nothing decided.**

**Goal.** Job scheduling and execution show up as spans, so a job can be followed from the request that scheduled it
to its execution, with everything the handler does nested inside.

**Idea.**
- `ActivitySource("JobMaster")`; users opt in with `.WithTracing(t => t.AddSource("JobMaster"))`. No OTel reference;
  netstandard2.0 needs the `System.Diagnostics.DiagnosticSource` package.
- A **producer span** for each scheduling call; a **consumer span** for each execution attempt (attempt number, error
  status on failure).
- The handler's own spans (EF Core, HttpClient, …) nest under the execution span automatically, and logs written during
  the job (F1) get its TraceId/SpanId. Today logs have none: background runners have no `Activity`.

```
POST /payroll/run              (ASP.NET request span, the user's)
 └─ JobMaster schedule PayrollJob     (producer span)
        ┆ link, minutes or days later, other process
 JobMaster execute PayrollJob   (consumer span, one per attempt)
  ├─ SELECT employees           (user's EF Core span)
  └─ POST bank-api/transfer     (user's HttpClient span)
```

**Open decisions.**
- Where the trace context (`traceparent`) is stored on a job / recurring schedule: a dedicated field (clean, but a
  schema change across every provider and transport message) or a reserved metadata key (no schema change, but
  visible and overwritable by users).
- Execution span as a **link + new trace** (OTel messaging conventions; avoids huge traces when jobs run much later or
  retry) or as a **child** of the scheduling span.
- Recurring occurrences: a new trace per occurrence? Linked to the span that created the schedule (none for static
  schedules)?
- Internal runner spans (assign to buckets, save pending, drain…): noisy because they poll. Off by default, maybe a
  separate `JobMaster.Internal` source.
- Bulk scheduling: one producer span, each job links to it?

**Open for discussion: long-lived entities (bucket, worker, agent connection, host).** One span for an entity's whole
life doesn't fit: a span is exported only when it ends, is lost on a crash, and can't move between processes (a lost
bucket is taken over by another worker). Options, none chosen:
- lifecycle steps as **logs** with the entity id attribute (history = filter by id);
- short **spans per operation** on the entity (drain bucket, reassign lost bucket), each its own trace;
- **metrics** for counts and trends (buckets by status, lost buckets per hour).

**Metrics (later).** `Meter("JobMaster")`, e.g. jobs scheduled/succeeded/failed, execution duration, wait time before
start. No high-cardinality ids (job, execution, bucket) as metric attributes, since each distinct value is a new
time series; bounded sets only (cluster, job definition, worker lane, priority, status).

---

# Dashboard alerts

## F3. Dashboard alerts

**Status: idea registered, open for discussion.**

**Independent of OpenTelemetry.** No dependency in either direction: users can use the Dashboard alerts, the OTel
integration (and their own backend's alerting), or both.

**Goal.** Alert rules configured in the Dashboard, so teams without an observability stack (e.g. a small app in
standalone mode) get notified when something goes wrong, with no extra infrastructure.

**Parts.**

| Part | What it means in JobMaster |
|---|---|
| Rules | Stored on master, edited in the Dashboard. Fixed rule types with user-set thresholds and filters (see catalog). |
| Evaluator | A master-side runner checking the rules every N seconds. Exactly one per cluster (distributed lock / coordinator), or alerts fire twice. |
| Data | Queries on master: jobs, executions, heartbeats, logs. Adds load to the master DB, so queries must be cheap and indexed. |
| State | Firing / resolved, de-duplication, cooldown. Without it one incident sends hundreds of notifications. |
| Providers | Where notifications go (see below). Need stored settings / secrets. |
| Access | Who can edit rules: ties into the Dashboard OAuth/SSO roles. |

**Rule catalog (first ideas).**
- **Error count**: N errors in M min, optional log category / job definition. Source: the logs table (buffered 15s and
  dropped when the logger queue is full, so the count can lag or be low) or failed executions (more reliable for job
  failures).
- **No worker available** for X min, optional cluster / worker lane. Source: worker heartbeats
  (`ResourceHeartbeatType.AgentWorker`, `KeepAliveWorkerRunner`).
- **No bucket available** for X min, optional lane / priority. Gap: `AssignJobsToBucketsRunner` already detects it
  (fallback bucket after `NoBucketFallbackThreshold`, 2.5 min) but keeps the first-failure time in memory
  (`bucketAssignFirstFailure`), so the evaluator can't read it. Either persist it, or derive "jobs on master older than
  X with no bucket of that lane + priority".
- **Agent connection down** for X min (agent connection heartbeats).
- **Job waiting too long**: jobs on master with scheduled time < now − X (needs an indexed query).
- Shared cycle for every rule: evaluate every N seconds → the condition must hold for X before firing (no flapping) →
  one notification → a "resolved" notification when it clears.

**Providers.** Pluggable alert providers.
- **First version (free): email (SMTP) and HTTP POST** (JSON payload to any URL; also covers Slack/Teams incoming
  webhooks).
- **Paid version: every other provider**, e.g. chat apps directly, paging (PagerDuty, Opsgenie), tickets (Jira,
  GitHub issues), and "run a JobMaster job handler" as the action (user code, no secrets stored; can't run when no
  worker is available, so it needs a backup provider). The provider contract must let paid providers plug in from a
  separate package.

**Open decisions.**
- Provider contract shape (e.g. an alert-provider interface plus registration, like the storage-provider packages);
  free providers in core or in their own package.
- HTTP POST payload format; whether to send both firing and resolved notifications.
- Where SMTP settings / credentials and POST auth headers live: app config (leaning) vs stored in the Dashboard DB.
- Which rule types go in the first version.
- Evaluator placement (coordinator runner vs any worker with a lock); standalone mode.

---

# AI debug

## F4. AI debug (Dashboard chat)

**Status: idea registered (Hugo, 2026-10-27), open for discussion.**

**Goal.** From the Dashboard, a user opens an AI chat to investigate a JobMaster problem: "why did this job fail?",
"why is this worker not taking jobs?", "why does cluster-1 have no bucket for lane billing?". The AI queries JobMaster
itself to find the answer, instead of the user reading logs and pages by hand.

**Idea (Hugo).**
- **The user brings their own AI access.** They configure their AI provider's token (or whatever it needs).
  JobMaster doesn't pay for or proxy any AI usage.
- **JobMaster gives the AI skills (tools) over the REST API**: query logs, check a worker's status, read a job and its
  executions, list buckets, and so on.
- **Chats start from a resource or from nothing.**
  - **Linked chat**: opened from a job, recurring schedule, execution, worker, bucket, agent connection, host, cluster
    or a single log item. The chat starts with that resource as context.
  - **Free chat**: linked to nothing; the AI finds what it needs with the tools.
- **Scope: JobMaster only.** The AI is instructed to only investigate problems related to the JobMaster server, not
  act as a general-purpose assistant.
- **Source code access.** JobMaster is open source, so the AI can also be pointed at the JobMaster code (and docs) to
  answer "how does X work / why does JobMaster do Y" questions.

**Current state (relevant pieces).**
- `JobMaster.Api` endpoints are **all `GET`** (jobs, recurring schedules, logs, workers, buckets, agent connections,
  hosts, clusters, `whoami`). Tools built on them are read-only by construction.
- The Dashboard has a server side (`JobMaster.Dashboard/Endpoints`, OAuth/SSO login, auth retention) and an OpenAPI
  document (`OpenApi/OpenApiJsonConfigSeeder.cs`), which could describe the tools.
- Logs can be queried by `ReferenceGuid`, level, category, time range and keyword. `ApiLogItem` already has a
  `CutMessage()` truncation, useful to keep tool results small.

**Design notes (proposed, not decided).**
- **Where the chat runs:** on the Dashboard server, not the browser. The AI token never reaches the browser, and the
  tool calls run server side.
- **Permissions follow the logged-in user.** Tools call the API as the Dashboard user (same auth as the Dashboard
  pages), so the AI can never see more than that user can.
- **Tools:** one tool per useful API query (e.g. get job, list job executions, query logs, get worker, list buckets,
  get cluster). Possibly generated from the OpenAPI document, or hand-written with AI-oriented descriptions (usually
  gives better results). Tool results trimmed (truncated messages, paging) so a big log query doesn't blow the
  context.
- **Linked chat context:** the starting resource's details (and for a log item, its `ReferenceId` resource) loaded
  as the first message, so the AI starts from the right place.
- **Scope instructions** in the system prompt: JobMaster-only, read-only investigation, cite what it looked at (ids,
  log entries), say when it doesn't know. This is a soft limit (users can still push it off-topic); the hard limit
  is that the tools only read JobMaster data.
- **Source code:** a tool that reads/searches the JobMaster repo **at the version the user is running** (the
  package version maps to a git tag), so answers match their code, not `master`. Plus the docs site.

**Decided (Hugo).**
- **AI providers are pluggable**, like the alert providers (F3).
  - **Free version: one provider, Ollama** (probably).
  - **Paid version: the others**, e.g. Claude, added as separate providers.
  - Configured in the **Dashboard startup code** (with the rest of the Dashboard setup), not entered in the Dashboard
    UI. So the token / endpoint lives in app config and is never stored on master.
- **Skills are configured at startup too**, next to the AI provider: which skills the AI gets, e.g. access to GitHub
  repos (the JobMaster source at the running version, and possibly the user's own repos so the AI can read their
  handler code), the docs, and the built-in JobMaster API tools. Each skill brings its own settings (e.g. a
  read-only GitHub token). **Skills work like AI providers (Hugo):** the same pluggable model, registered in the
  Dashboard startup code, with a free set and more skills in the paid version (separate packages). Which skills are
  in the free set is open (e.g. the JobMaster API tools + JobMaster source/docs free; the user's own repos and other
  integrations paid?).
- **Where skill config lives (proposed, agreed by Hugo).** Three layers:
  - *which skills exist* (code, incl. paid packages) → startup code only;
  - *secrets* (GitHub token, AI key) → startup / app config, never stored on master;
  - *settings and on/off* (enable a skill, which repos, extra instructions for the AI, per cluster) → editable in
    the Dashboard, stored in the generic storage (same as chat history / F3 rules), admin-only, and only inside what
    startup allows (can't enable an unregistered skill).
  - Today `DashboardOptions` is startup-only and served read-only (`jobmaster-config.json`), so Dashboard editing is
    new; startup-only is fine for a first version.
- **Skill shape (proposed).** A skill = **instructions** (when to use it, how to investigate, e.g. "stuck job:
  check status → executions → logs by `ReferenceGuid` → bucket/worker heartbeat") + **tools** (name, description,
  input schema, executor) + **settings**. Every tool declares its **kind**:
  - `Read`: reads JobMaster data, no side effects (all current API tools);
  - `External`: reads outside JobMaster (GitHub, docs site);
  - `Write`: changes something (cancel, reschedule). Not in the first version; if added later, each call needs
    explicit user confirmation in the chat (prompt-injection guard).
  Similar to MCP tool annotations (`readOnlyHint`, `destructiveHint`, `openWorldHint`), which helps if the same tools
  are exposed as an MCP server later.
- Ollama as the free provider also helps with data privacy: a self-hosted model keeps job data on the user's own
  infrastructure. The paid providers (cloud AI) are where the "data leaving the system" question applies.

**Chat history (main open issue).** Not designed yet. Hugo's first idea: **the API provides a generic storage**.
- There's an existing mechanism to build on: master's **generic records** (`IMasterGenericRecordRepository`,
  `GenericRecordEntry`, `MasterGenericRecordGroupIds`), already used for hosts, workers, buckets, agent connections,
  heartbeats and metadata, on every provider. It has what chat history needs:
  - a new group id (e.g. `AiChat` + `AiChatMessage`);
  - value filters + paging (`GenericRecordQueryCriteria`), e.g. "chats linked to job X", "chats of user Y";
  - `ExpiresAt` + `DeleteExpiredAsync` / `DeleteByCreatedAtAsync`, so old chats can be purged by a retention setting.
- The same storage could also hold the alert rules and alert state of F3, so one generic storage serves both
  Dashboard features.
- To settle:
  - one record per message (append-only, small writes) vs one record per chat (rewritten on every message);
  - size: tool results can be large, so store them trimmed or not at all (keep only the AI's answers + which tools it
    called)?
  - **Decided (Hugo): cluster oriented, like the API.** Every API route is `/{clusterId}/{entity}`, and generic
    records are per cluster, so **every chat belongs to one cluster** and is stored on that cluster's master. Chat
    endpoints follow the same shape, e.g. `/{clusterId}/ai-chats`. A free chat is "linked to nothing" inside the
    cluster the user has selected in the Dashboard; a linked chat belongs to its resource's cluster.
    - **Decided (Hugo): the AI can look at other clusters** from inside a chat (e.g. a migrating / archived cluster
      whose jobs moved to the target cluster). Tools take a `clusterId`, defaulting to the chat's cluster, and are
      limited to the clusters the logged-in user can access (permissions follow the user). The chat itself is still
      stored on its own cluster.
  - who can see whose chats (owner only / everyone with Dashboard access), tied to the OAuth/SSO identity;
  - is the storage exposed as a generic API endpoint (any key/value document for the Dashboard) or as chat-specific
    endpoints over generic records?
  - retention default (e.g. 30 days) and a way to delete a chat.

**Open decisions.**
- **Ollama only, or "OpenAI-compatible endpoint"?** Ollama speaks the OpenAI-compatible API, so a generic
  OpenAI-compatible provider would also cover many paid cloud APIs. Keep the free provider Ollama-specific if the
  paid split matters.
- **Tool-calling quality:** small local models are weaker at multi-step tool use than large cloud models. Test the
  investigation flow on a few Ollama models before promising it; maybe recommend a minimum model in the docs.
- **Data leaving the system** (paid / cloud providers): message data, metadata and log messages can hold customer
  data. Opt-in per cluster? An option to leave out message data / metadata from tool results?
- **Prompt injection:** log messages, job data and metadata are user-controlled text that the AI reads. While the
  tools only read, the damage is limited (a misleading answer). This becomes a real risk if write tools (cancel,
  reschedule) are ever added; keep them out, or require explicit user confirmation per action.
- **Limits:** max tool calls per answer, token budget per chat, rate limit per user. The user pays for the tokens, so
  make the cost visible.
- **Source code access mechanism:** a GitHub skill configured at startup (decided above) fetches the JobMaster repo
  at the running version's tag; needs network access from the Dashboard server. Open: an offline fallback (an index
  shipped with the package)? For the user's own repos, keep the token read-only and scoped to the repos they list.
- Relation to an MCP server: the same tool set could be exposed as an MCP server so users can investigate from their
  own AI tools (Claude Code, Cursor…). Later item, or the same tool layer from day one?

---

# Generic storage API

## F5. Generic storage API (custom groups)

**Status: idea registered (Hugo, 2026-10-28), shape partly decided.**

**Goal.** Let the Dashboard features (F4 chat history and skill settings, F3 alert rules and state), and possibly
users, store their own documents on a cluster's master through `JobMaster.Api`, on top of the existing generic
records. Carefully: it must never expose or collide with JobMaster's own data.

**Current state.**
- Master's generic records (`IMasterGenericRecordRepository`, `GenericRecordEntry`) already work on every provider:
  get / query (value filters, paging) / insert / upsert / delete, `ExpiresAt` + `DeleteExpiredAsync`,
  `DeleteByCreatedAtAsync`.
- Built-in groups (`MasterGenericRecordGroupIds`): `ClusterConfiguration`, `Host`, `AgentWorker`, `Bucket`,
  `AgentConnection`, `Sentinel`, the heartbeat groups, `JobMasterMetadata`, `RecurringScheduleMetadata`. They're only
  reachable through their own API endpoints (`/{clusterId}/buckets`, …), never as raw records.
- API identity: `GetResolvedIdentity` → `Subject` + `AuthenticationType` (API key name or JWT `sub`), as returned by
  `GET /whoami`. No roles today.

**Decided (Hugo).**
- **Custom groups only.** The generic API only serves groups registered as custom. Built-in groups (topology,
  heartbeats, metadata, …) stay behind their own endpoints, exactly as today, and are never reachable through it.
- **Register before use:**
  ```
  POST {clusterId}/generic/{groupId}/register-group     → makes the group available
  POST/GET/... {clusterId}/generic/{groupId}/...        → only works for a registered group, otherwise 404
  ```
- **Prefixed storage names**, so custom groups can never collide with built-in ones (today or added later): the
  stored `GroupId` is e.g. `custom:<groupId>`. Hard validation: a custom `groupId` can't contain the separator, can't
  match a built-in group name, and must match a strict charset/length (it's part of a URL).
- **Owner + shared flag on every record.** Every record has an owner, and a `Shared` flag. Not shared → only the
  owner can access it (Joao's chats aren't visible to Maria). Shared → everybody with access to the cluster can
  access it, e.g. alert rules shared across the team.
- **Scope principle.** All information is derived from a cluster: generic records live on one cluster's master.
  Settings and data outside a cluster's scope aren't stored by JobMaster; they're registered / configured in
  startup code.

**Design notes (proposed).**
- **Group registration record** (in a built-in registry group, not reachable by the generic API) holds the group's
  policy, set at registration:
  - **default for the `Shared` flag** on new records (chats: not shared; alert rules: shared). Per record, the owner
    can change it;
  - **default TTL** (e.g. chats 30 days → `ExpiresAt` on insert, purged by the existing expiry cleanup);
  - **limits**: max record size, max records per owner.
- **Owner = `AuthenticationType` + `Subject`**, stored on each record and set by the server from the caller's
  identity, never from the request body. Both parts, because an API key named `joao` and a JWT with `sub` `joao` are
  different callers. Queries return the caller's own records plus shared ones, never other owners' private
  records.
- **Initial version (Hugo): keep it simple.** Shared = everybody **reads**; only the owner writes / deletes.
  Later: richer modes (e.g. `EveryoneCanRead`, `EveryoneCanWrite`). Alert rules are the likely first candidate for
  "others can edit". Hugo is still thinking about it.
- **Owner lives in the API, as pluggable filters (Hugo, latest).** The generic API core stays a plain store of
  custom-group records; owner / sharing is additional code plugged into it as **record filters** (a
  middleware-like pipeline on the generic endpoints):
  - the built-in owner filter sets `owner = <caller identity>` on writes and adds the owner / shared condition to
    reads, before the query reaches the repository;
  - **the owner value always comes from the authenticated identity** (`AuthenticationType` + `Subject`), never from
    the client. A client-sent `owner=` can only narrow the result to the caller's own records; any other owner →
    ignored / 403, never widens access;
  - **the pro version plugs more filters / rules into the same pipeline** (roles, team access, edit rights on shared
    records), without changing the core;
  - filters are chained (all apply), unlike the single `IJobMasterAuthorizationProvider`.
  - The Dashboard keeps calling the API directly from the browser; no Dashboard-side proxy needed. (A proxy design,
    Dashboard server enforcing owner with a service key, was discussed and set aside.)
- **Authorization (checked).** The API already has a public hook: `IJobMasterAuthorizationProvider.IsAuthorizedAsync(identity,
  HttpContext)`, registered with `UseCustomizeJobMasterAuthorizationProvider<T>()`, run on every request by the
  endpoint filter in `MapJobMasterApi` (when `RequireAuthentication` is on). It sees the identity (`Subject`,
  `AuthenticationType`, `Claims`, e.g. JWT roles) and the route / method, so request-level rules ("only
  `role=admin` may `register-group`", "team X only on cluster-2") already work today.
  - Limit 1: it's yes/no for the whole request. It can't decide *which records* a query returns, so owner / shared
    checks live in the generic endpoints, behind their own record-level hook.
  - Limit 2: one provider only; a custom one replaces the default. A paid rules engine shipped as a provider would
    replace the user's own provider. Before that, make providers chainable (all must allow) or give the rules engine
    its own hook point.
  - Paid version (Hugo): richer authorization rules (roles, per-group / per-cluster permissions, edit rights on
    shared records) built on these two hooks.
- **Endpoints** (sketch): register / get group info; insert, get by id, query (owner-filtered, paged, a small set of
  value filters), update, delete. Cluster oriented like the rest of the API: one cluster's master per call.
- Same naming care as the hybrid connection reserved strings (`hybrid:`, `&&`): the custom prefix becomes
  reserved.

**Global scope: dropped (Hugo).** A "global cluster" (e.g. a `jm-global`-prefixed id, with no workers) doesn't
work: each cluster has its own master DB, so there's no shared place to store cross-cluster data. Everything in F5
stays per cluster. (A variant that stores "global" data on one configured home cluster was discussed but not taken
forward.)

**Migration and generic records (checked).** The migrate runners (`MigrateJobsRunner`,
`MigrateRecurringSchedulesRunner`) only move jobs and recurring schedules, never generic records. So custom records
(F4 chats, F3 rules) stay on a migrated / archived cluster's master unless a new step copies them. Open: copy them,
or accept and document it. Expired records are already purged by `DeleteExpiredGenericRecordsRunner`, so per-group
TTLs need no new runner.

**Open decisions.**
- **Who can register a group?** No roles exist today. Options: only from startup code (the Dashboard registers
  `ai-chat`, `alert-rule` at startup, no public endpoint needed), or the endpoint restricted to an admin API key /
  claim. The endpoint as decided needs some admin notion.
- **Admin override:** can an admin read other owners' private (not shared) records (support, audits)?
- Can a registration be changed (policy, TTL) or deleted, and what happens to the existing records?
- Exposed to end users as a public feature, or internal for the Dashboard features first?
- Which value filters the query endpoint allows (indexes; generic records live in a shared table, so keep queries
  cheap).
- Prefix format: `custom:` vs another reserved prefix; settle together with the other reserved strings.

**Tasks (when picked up).**
- [ ] Registry group + registration endpoint + policy model; reserved-prefix and built-in-name validation.
- [ ] CRUD/query endpoints with owner stamping and owner filtering; 404 for unregistered groups.
- [ ] TTL default on insert; limits.
- [ ] Unit tests: unregistered group → 404; built-in name can't be registered or reached; `OwnerOnly` isolation
      (Joao can't read Maria's records); owner never taken from the body; TTL applied.
- [ ] RepoConformance: custom-prefixed groups round-trip on all providers.
- [ ] API docs + ChangeLog.

---

# Fixes found while planning

## X1. Dashboard OpenAPI seeder trusts the `Host` header

**Status: bug, to fix (Hugo, 2026-10-28).** Not a future feature; listed here so it isn't lost.

**Problem.** `OpenApiJsonConfigSeeder.SeedAsync` ([OpenApiJsonConfigSeeder.cs](src/JobMaster.Dashboard/OpenApi/OpenApiJsonConfigSeeder.cs)),
for a relative `ApiUrl` / `OpenApiUrl`, builds the URL it fetches from the incoming request:
`$"{scheme}://{host}{baseApiUrl}/openapi/..."`. The `Host` header is set by whoever sends the request, so a request
with `Host: attacker.example` makes the Dashboard server fetch the OpenAPI document from the attacker's server.
- To check: is the seeded result cached and shared? If so, one crafted request changes what every user gets.
- Any future server-to-API call (F4 tools, anything sending a credential) must never reuse this pattern, or the
  credential goes to the attacker's host.

**Fix (proposed).** Build server-to-API URLs only from configuration: require an absolute URL for server-side calls
(or a separate internal API base URL setting), or resolve relative paths against a configured / allow-listed host,
never `HttpContext.Request.Host`.

**Tasks.**
- [ ] Remove the `Host`-derived URL; configured base URL only; clear startup error if a relative URL can't be resolved.
- [ ] Unit test: a request with a forged `Host` never changes the fetched URL.
- [ ] ChangeLog (security fix).
