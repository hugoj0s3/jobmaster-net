<script lang="ts">
    type JobStatus =
        | "Processing"
        | "Queued"
        | "SavePending"
        | "OnMaster"
        | "InBucket"
        | "Succeeded"
        | "Failed"
        | "Cancelled";

    type Priority = "Critical" | "High" | "Medium" | "Low";

    type Job = {
        jobId: string;
        definitionId: string;
        metadata: Record<string, string>;
        status: JobStatus;
        priority: Priority;

        executedAt?: string;
        scheduledAt?: string;

        host?: string;
        worker?: string;
        bucket?: string;
    };

    let refresh = true;

    const jobs: Job[] = [
        {
            jobId: "8b12...f9a7",
            definitionId: "FetchDataHandler",
            metadata: { "!lane": "DataProcessing", "!tenant": "acme", debug: "true" },
            status: "Processing",
            priority: "Critical",
            scheduledAt: new Date(Date.now() - 2 * 60_000).toISOString(),
            host: "Host 3",
            worker: "Payroll-Worker-02"
        },
        {
            jobId: "f19b...770e",
            definitionId: "SendEmailHandler",
            metadata: { "!tenant": "acme", trace: "on" },
            status: "OnMaster",
            priority: "High",
            scheduledAt: new Date(Date.now() + 12 * 60_000).toISOString(),
            host: "Host 3"
        },
        {
            jobId: "50d2...de4c",
            definitionId: "MethodHandler",
            metadata: { "!lane": "Invoicing" },
            status: "InBucket",
            priority: "Medium",
            scheduledAt: new Date(Date.now() + 90 * 60_000).toISOString(),
            bucket: "bucket-12"
        },
        {
            jobId: "717e...7d7a",
            definitionId: "InvoicingHandler",
            metadata: { "!tenant": "beta", note: "x" },
            status: "Failed",
            priority: "Low",
            executedAt: new Date(Date.now() - 61 * 60_000).toISOString(),
            host: "Host 1",
            worker: "DNS-Worker-01"
        },
        {
            jobId: "9aa1...1cc2",
            definitionId: "CleanupHandler",
            metadata: { "!tenant": "acme" },
            status: "Succeeded",
            priority: "Low",
            executedAt: new Date(Date.now() - 70_000).toISOString(),
            host: "Host 2",
            worker: "Cleanup-Worker-01",
            bucket: "bucket-2"
        }
    ];

    const statusClasses: Record<JobStatus, string> = {
        Processing: "badge-info",
        Queued: "badge-warning",
        SavePending: "badge-neutral",
        OnMaster: "badge-ghost",
        InBucket: "badge-secondary",
        Succeeded: "badge-success",
        Failed: "badge-error",
        Cancelled: "badge-ghost"
    };

    const priorityClasses: Record<Priority, string> = {
        Critical: "badge-error",
        High: "badge-warning",
        Medium: "badge-info",
        Low: "badge-neutral"
    };

    let selected = new Set<string>();

    $: allSelected = jobs.length > 0 && selected.size === jobs.length;
    $: someSelected = selected.size > 0 && selected.size < jobs.length;

    function toggleAll(checked: boolean) {
        selected = checked ? new Set(jobs.map((j) => j.jobId)) : new Set();
    }

    function toggleOne(jobId: string, checked: boolean) {
        const next = new Set(selected);
        if (checked) next.add(jobId);
        else next.delete(jobId);
        selected = next;
    }

    function formatIso(iso: string) {
        const d = new Date(iso);
        return d.toLocaleString();
    }

    function diffMinutes(fromMs: number, toMs: number) {
        return Math.round((toMs - fromMs) / 60_000);
    }

    function formatTimeCell(job: Job): { label: string; tooltip?: string } {
        const now = Date.now();

        if (job.status === "Succeeded" || job.status === "Failed" || job.status === "Cancelled") {
            if (!job.executedAt) return { label: "Executed", tooltip: undefined };
            const executedMs = new Date(job.executedAt).getTime();
            const minsAgo = Math.max(0, diffMinutes(executedMs, now));
            return {
                label: `Executed ${minsAgo}m ago`,
                tooltip: formatIso(job.executedAt)
            };
        }

        if (job.scheduledAt) {
            const scheduledMs = new Date(job.scheduledAt).getTime();
            if (scheduledMs > now) {
                const mins = diffMinutes(now, scheduledMs);
                if (mins <= 90) {
                    return { label: `Scheduled in ${mins}m`, tooltip: formatIso(job.scheduledAt) };
                }
                return { label: `Scheduled at ${formatIso(job.scheduledAt)}`, tooltip: formatIso(job.scheduledAt) };
            }

            const delayedMins = Math.max(0, diffMinutes(scheduledMs, now));
            return { label: `Delayed ${delayedMins}m`, tooltip: formatIso(job.scheduledAt) };
        }

        return { label: "—", tooltip: undefined };
    }

    function metadataPairs(job: Job): Array<[string, string]> {
        return Object.entries(job.metadata).filter(([k]) => k.startsWith("!"));
    }

    function canRunAgain(job: Job) {
        return job.status === "Succeeded" || job.status === "Failed" || job.status === "Cancelled";
    }

    function canRunNow(job: Job) {
        return job.status === "OnMaster" || job.status === "InBucket";
    }

    function canCancel(job: Job) {
        return job.status === "OnMaster";
    }

    function canAbort(_job: Job) {
        return true;
    }

    $: selectedJobs = jobs.filter((j) => selected.has(j.jobId));
    $: bulkRunAgainEnabled = selectedJobs.length > 0 && selectedJobs.every(canRunAgain);
    $: bulkRunNowEnabled = selectedJobs.length > 0 && selectedJobs.every(canRunNow);
    $: bulkCancelEnabled = selectedJobs.length > 0 && selectedJobs.every(canCancel);
    $: bulkAbortEnabled = selectedJobs.length > 0 && selectedJobs.every(canAbort);

    function bulkRunAgain() {
        console.log("Bulk Run Again", [...selected]);
    }

    function bulkRunNow() {
        console.log("Bulk Run Now", [...selected]);
    }

    function bulkCancel() {
        console.log("Bulk Cancel", [...selected]);
    }

    function bulkAbort() {
        console.log("Bulk Abort", [...selected]);
    }
</script>

<div class="min-h-screen bg-base-100">
    <div
            class="pointer-events-none fixed inset-0 opacity-50"
            style="
      background:
        radial-gradient(1200px 600px at 30% 10%, rgba(45,212,191,0.10), transparent 60%),
        radial-gradient(900px 500px at 70% 20%, rgba(96,165,250,0.10), transparent 60%),
        radial-gradient(900px 500px at 80% 80%, rgba(167,139,250,0.10), transparent 60%);
    "
    />

    <div class="relative w-full px-6 py-6">
        <div class="flex items-center justify-between gap-4">
            <h1 class="text-4xl font-semibold">Jobs</h1>

            <div class="flex items-center gap-3 text-sm opacity-80">
                <span>Last updated: 10s ago</span>

                <label class="flex items-center gap-2 cursor-pointer select-none">
                    <input
                            type="checkbox"
                            bind:checked={refresh}
                            class="toggle toggle-sm toggle-primary"
                    />
                    <span class={` font-semibold text-sm ${refresh ? "text-primary" : ""} `}>Refresh</span>
                </label>

                <button class="btn btn-ghost btn-sm btn-square" aria-label="More">
                    <svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5" fill="currentColor" viewBox="0 0 24 24">
                        <circle cx="12" cy="5" r="2" />
                        <circle cx="12" cy="12" r="2" />
                        <circle cx="12" cy="19" r="2" />
                    </svg>
                </button>
            </div>
        </div>

        {#if selected.size > 0}
            <div
                    class="mt-4 flex flex-wrap items-center justify-between gap-3 rounded-box border border-base-300 bg-base-200/60 px-4 py-3 backdrop-blur"
            >
                <div class="text-sm opacity-80">
                    <span class="font-semibold">{selected.size}</span>
                    selected
                </div>

                <div class="flex flex-wrap gap-2">
                    <button class="btn btn-sm" on:click={bulkCancel} disabled={!bulkCancelEnabled}>
                        Cancel ({selected.size})
                    </button>
                    <button class="btn btn-sm" on:click={bulkAbort} disabled={!bulkAbortEnabled}>
                        Abort ({selected.size})
                    </button>
                    <button class="btn btn-sm" on:click={bulkRunNow} disabled={!bulkRunNowEnabled}>
                        Run Now ({selected.size})
                    </button>
                    <button class="btn btn-sm" on:click={bulkRunAgain} disabled={!bulkRunAgainEnabled}>
                        Run Again ({selected.size})
                    </button>

                    <button class="btn btn-ghost btn-sm" on:click={() => (selected = new Set())}>
                        Clear
                    </button>
                </div>
            </div>
        {/if}

        <div class="mt-4 card bg-base-200/50 shadow-xl backdrop-blur">
            <div class="overflow-x-auto">
                <table class="table">
                    <thead class="opacity-70">
                    <tr>
                        <th class="w-10">
                            <input
                                    type="checkbox"
                                    class="checkbox checkbox-sm"
                                    checked={allSelected}
                                    indeterminate={someSelected}
                                    on:change={(e) => toggleAll((e.currentTarget as HTMLInputElement).checked)}
                                    aria-label="Select all"
                            />
                        </th>
                        <th>JobId</th>
                        <th>DefinitionId</th>
                        <th>Metadata</th>
                        <th>Status</th>
                        <th>Priority</th>
                        <th>Time</th>
                        <th>Host</th>
                        <th>Worker</th>
                        <th>Bucket</th>
                    </tr>
                    </thead>

                    <tbody>
                    {#each jobs as j (j.jobId)}
                        <tr class="hover">
                            <td>
                                <input
                                        type="checkbox"
                                        class="checkbox checkbox-sm"
                                        checked={selected.has(j.jobId)}
                                        on:change={(e) => toggleOne(j.jobId, (e.currentTarget as HTMLInputElement).checked)}
                                        aria-label={`Select ${j.jobId}`}
                                />
                            </td>

                            <td class="font-medium">
                                <a class="link link-hover" href={`/jobs/${j.jobId}`}>{j.jobId}</a>
                            </td>

                            <td>{j.definitionId}</td>

                            <td>
                                {#if metadataPairs(j).length === 0}
                                    <span class="opacity-60">—</span>
                                {:else}
                                    <div class="flex flex-wrap gap-2">
                                        {#each metadataPairs(j) as [k, v] (k)}
                                            <span class="badge badge-ghost badge-sm">{k}={v}</span>
                                        {/each}
                                    </div>
                                {/if}
                            </td>

                            <td>
									<span class={`badge badge-sm ${statusClasses[j.status] ?? "badge-ghost"}`}>
										{j.status}
									</span>
                            </td>

                            <td>
									<span class={`badge badge-sm ${priorityClasses[j.priority] ?? "badge-ghost"}`}>
										{j.priority}
									</span>
                            </td>

                            <td>
                                {#if formatTimeCell(j).tooltip}
										<span class="tooltip tooltip-bottom" data-tip={formatTimeCell(j).tooltip}>
											{formatTimeCell(j).label}
										</span>
                                {:else}
                                    <span>{formatTimeCell(j).label}</span>
                                {/if}
                            </td>

                            <td>
                                {#if j.host}
                                    <span class="tooltip tooltip-bottom" data-tip={j.host}>{j.host}</span>
                                {:else}
                                    <span class="opacity-60">—</span>
                                {/if}
                            </td>

                            <td>
                                {#if j.worker}
                                    <span class="tooltip tooltip-bottom" data-tip={j.worker}>{j.worker}</span>
                                {:else}
                                    <span class="opacity-60">—</span>
                                {/if}
                            </td>

                            <td>
                                {#if j.bucket}
                                    <span class="tooltip tooltip-bottom" data-tip={j.bucket}>{j.bucket}</span>
                                {:else}
                                    <span class="opacity-60">—</span>
                                {/if}
                            </td>
                        </tr>
                    {/each}
                    </tbody>
                </table>
            </div>

            <div class="flex items-center justify-between px-6 py-4 opacity-80">
                <div class="text-sm">
                    Showing <span class="font-semibold">{jobs.length}</span> jobs
                </div>

                <div class="flex gap-2">
                    <button class="btn btn-ghost btn-sm" disabled>‹</button>
                    <button class="btn btn-ghost btn-sm" disabled>›</button>
                </div>
            </div>
        </div>
    </div>
</div>