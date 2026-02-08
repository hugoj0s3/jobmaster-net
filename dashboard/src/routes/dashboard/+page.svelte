<script lang="ts">
  const lifecycle = [
    { label: "SavePending", value: 0, cls: "bg-base-300/60 text-base-content/80" },
    { label: "HeldOnMaster", value: 14, cls: "bg-base-300/60 text-base-content/80" },
    { label: "...", value: 27, cls: "bg-warning/80 text-black" },
    { label: "Assignee164", value: 0, cls: "bg-warning/60 text-black" },
    { label: "Queued", value: 8, cls: "bg-info/70 text-black" },
    { label: "Processing", value: 4, cls: "bg-info/50 text-black" },
    { label: "Succeeded", value: "1.1k", cls: "bg-success/70 text-black" },
    { label: "Failed", value: 5, cls: "bg-error/80 text-black" }
  ];

  const recent = [
    { status: "Succeeded", title: "Fetch Data Job", meta: "145ms", ago: "3m ago" },
    { status: "Succeeded", title: "Generate Report", meta: "5.2s", ago: "5m ago" },
    { status: "Failed", title: "Send Email Notification", meta: "2.3s", ago: "14m ago" },
    { status: "Succeeded", title: "Process Invoices", meta: "1.2s", ago: "22m ago" },
    { status: "Succeeded", title: "Fetch Data Job", meta: "121ms", ago: "26m ago" }
  ];

  const issues = [
    { n: 1, text: "Lost bucket detected", action: "View buckets" },
    { n: 4, text: "Worker Payroll-Worker-02 offline", action: "View workers" }
  ];

  const workers = [
    { name: "DNS-Worker-01", mode: "Full", lane: "Default", hb: "12s", jobs: 2 },
    { name: "FileImport-Worker-01", mode: "Coordinator", lane: "FileImport", hb: "9s", jobs: 0 },
    { name: "Payroll-Worker-01", mode: "Execution", lane: "Payroll", hb: "7s", jobs: 1 }
  ];

  function StatusIcon({ status }: { status: string }) {
    // placeholder; used via {#if} blocks below
    return status;
  }
</script>

<div class="min-h-screen bg-base-100">
  <!-- subtle top glow -->
  <div class="pointer-events-none fixed inset-0 opacity-50"
       style="background: radial-gradient(1200px 600px at 30% 10%, rgba(45,212,191,0.10), transparent 60%),
                           radial-gradient(900px 500px at 70% 20%, rgba(96,165,250,0.10), transparent 60%),
                           radial-gradient(900px 500px at 80% 80%, rgba(167,139,250,0.10), transparent 60%);">
  </div>

  <div class="relative mx-auto w-full max-w-6xl px-6 py-10">
    <!-- Header -->
    <div class="flex flex-col gap-4 md:flex-row md:items-center md:justify-between">
      <div class="flex items-center gap-3">
        <h1 class="text-4xl font-semibold tracking-tight text-base-content">Overview</h1>
        <div class="badge badge-outline px-3 py-3 text-xs opacity-90">Cluster: QA - Testing</div>
        <div class="badge badge-primary badge-lg font-semibold text-black">ACTIVE</div>
      </div>

      <div class="flex items-center gap-3 text-sm opacity-80">
        <span>Last updated: 12s ago</span>
        <button class="btn btn-ghost btn-sm gap-2">
          <!-- refresh icon -->
          <svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <path d="M21 12a9 9 0 1 1-3-6.7" />
            <path d="M21 3v6h-6" />
          </svg>
          Refresh
        </button>
        <button class="btn btn-ghost btn-sm btn-square" aria-label="Settings">
          <!-- gear -->
          <svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <path d="M12 15.5A3.5 3.5 0 1 0 12 8.5a3.5 3.5 0 0 0 0 7z"/>
            <path d="M19.4 15a7.7 7.7 0 0 0 .1-1l2-1.2-2-3.5-2.3.5a7.2 7.2 0 0 0-.8-.7l.3-2.4H9.7L10 5.1a7.2 7.2 0 0 0-.8.7L6.9 5.3l-2 3.5L7 10a7.7 7.7 0 0 0 0 2l-2.1 1.2 2 3.5 2.3-.5c.3.3.5.5.8.7l-.3 2.4h6.9l-.3-2.4c.3-.2.6-.4.8-.7l2.3.5 2-3.5-2-1.2z"/>
          </svg>
        </button>
      </div>
    </div>

    <!-- Top cards -->
    <div class="mt-8 grid grid-cols-1 gap-5 md:grid-cols-4">
      <!-- Jobs in progress -->
      <div class="card bg-base-200/70 shadow-xl backdrop-blur">
        <div class="card-body">
          <div class="flex items-start justify-between">
            <div>
              <div class="text-sm opacity-80">Jobs In Progress</div>
              <div class="mt-2 text-5xl font-semibold">12</div>
              <div class="mt-1 text-sm opacity-70">in-flight now</div>
            </div>
            <div class="opacity-80">
              <!-- circular spinner-ish -->
              <svg xmlns="http://www.w3.org/2000/svg" class="h-10 w-10" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <path d="M21 12a9 9 0 1 1-9-9" />
              </svg>
            </div>
          </div>
        </div>
      </div>

      <!-- Failed jobs (24h) - FIXED ICON: use X-in-circle instead of lightning -->
      <div class="card bg-base-200/70 shadow-xl backdrop-blur">
        <div class="card-body">
          <div class="flex items-start justify-between">
            <div>
              <div class="text-sm opacity-80">Failed Jobs (24h)</div>
              <div class="mt-2 text-5xl font-semibold text-error">5</div>
              <div class="mt-1 text-sm opacity-70">/ today</div>
            </div>
            <div class="text-error opacity-90">
              <!-- X circle -->
              <svg xmlns="http://www.w3.org/2000/svg" class="h-10 w-10" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <circle cx="12" cy="12" r="9" />
                <path d="M15 9l-6 6" />
                <path d="M9 9l6 6" />
              </svg>
            </div>
          </div>
        </div>
      </div>

      <!-- Workers online -->
      <div class="card bg-base-200/70 shadow-xl backdrop-blur">
        <div class="card-body">
          <div class="flex items-start justify-between">
            <div>
              <div class="text-sm opacity-80">Workers <span class="text-primary">Online</span></div>
              <div class="mt-2 text-5xl font-semibold">4 / 4</div>
              <div class="mt-1 text-sm opacity-70">heartbeats ≅ 12s</div>
            </div>
            <div class="opacity-80">
              <!-- pulse -->
              <svg xmlns="http://www.w3.org/2000/svg" class="h-10 w-10" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <path d="M3 12h4l2-5 4 10 2-5h6" />
              </svg>
            </div>
          </div>
        </div>
      </div>

      <!-- Buckets -->
      <div class="card bg-base-200/70 shadow-xl backdrop-blur">
        <div class="card-body">
          <div class="flex items-start justify-between">
            <div>
              <div class="text-sm opacity-80">Buckets</div>
              <div class="mt-2 text-5xl font-semibold">24 / 24</div>
              <div class="mt-1 text-sm opacity-70">/ 0 lost, 1 draining</div>
            </div>
            <div class="opacity-80">
              <!-- trash-ish -->
              <svg xmlns="http://www.w3.org/2000/svg" class="h-10 w-10" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <path d="M3 6h18" />
                <path d="M8 6V4h8v2" />
                <path d="M6 6l1 16h10l1-16" />
              </svg>
            </div>
          </div>
        </div>
      </div>
    </div>

    <!-- Lifecycle pills -->
    <div class="mt-8">
      <div class="mb-3 text-sm font-semibold opacity-80">Job Lifecycle</div>
      <div class="flex flex-wrap gap-2">
        {#each lifecycle as item}
          <div class={"badge badge-lg rounded-full px-5 py-4 " + item.cls}>
            <span class="mr-2 opacity-90">{item.label}</span>
            <span class="font-semibold">{item.value}</span>
          </div>
        {/each}
      </div>
    </div>

    <!-- Bottom grid -->
    <div class="mt-8 grid grid-cols-1 gap-6 md:grid-cols-2">
      <!-- Left column -->
      <div class="flex flex-col gap-6">
        <div class="card bg-base-200/70 shadow-xl backdrop-blur">
          <div class="card-body">
            <div class="flex items-center justify-between">
              <div class="text-lg font-semibold">Top Workers</div>
              <button class="btn btn-ghost btn-sm btn-square opacity-70">…</button>
            </div>

            <div class="mt-2 space-y-4">
              <div class="flex items-center justify-between">
                <div class="flex items-center gap-3">
                  <div class="badge badge-error badge-lg rounded-full text-black">4</div>
                  <div class="text-sm opacity-90">Lost bucket detected</div>
                  <div class="text-xs opacity-50">3s ago</div>
                </div>
                <a class="link link-primary text-sm">View buckets</a>
              </div>

              <div class="flex items-center justify-between">
                <div class="flex items-center gap-3">
                  <div class="badge badge-warning badge-lg rounded-full text-black">1</div>
                  <div class="text-sm opacity-90">Worker Payroll-Worker-02</div>
                  <div class="text-xs opacity-50">offline</div>
                </div>
                <a class="link link-primary text-sm">View workers</a>
              </div>
            </div>
          </div>
        </div>

                <div class="card bg-base-200/70 shadow-xl backdrop-blur">
                  <div class="card-body">
                    <div class="text-lg font-semibold">Worker</div>

                    <div class="mt-4 overflow-x-auto">
                      <table class="table">
                        <thead class="opacity-60">
                          <tr>
                            <th>Name</th>
                            <th>Mode</th>
                            <th>Lane</th>
                            <th>Heartbeat</th>
                            <th>Jobs</th>
                          </tr>
                        </thead>
                        <tbody>
                          {#each workers as w}
                            <tr class="hover">
                              <td class="font-medium">{w.name}</td>
                              <td>{w.mode}</td>
                              <td>{w.lane}</td>
                              <td>{w.hb}</td>
                              <td>{w.jobs}</td>
                            </tr>
                          {/each}
                        </tbody>
                      </table>
                    </div>

                  </div>
                </div>
      </div>

      <!-- Right column -->
      <div class="flex flex-col gap-6">
        <div class="card bg-base-200/70 shadow-xl backdrop-blur">
          <div class="card-body">
            <div class="flex items-center justify-between">
              <div class="text-lg font-semibold">Recent Activity</div>
              <button class="btn btn-ghost btn-sm btn-square opacity-70">…</button>
            </div>

            <div class="mt-2 divide-y divide-base-300/60">
              {#each recent as r}
                <div class="flex items-center justify-between py-4">
                  <div class="flex items-center gap-3">
                    {#if r.status === "Succeeded"}
                      <span class="text-success">
                        <!-- check -->
                        <svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="3">
                          <path d="M20 6L9 17l-5-5" />
                        </svg>
                      </span>
                    {:else}
                      <span class="text-error">
                        <!-- x -->
                        <svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="3">
                          <path d="M18 6L6 18" />
                          <path d="M6 6l12 12" />
                        </svg>
                      </span>
                    {/if}

                    <div class="text-sm">
                      <span class={r.status === "Failed" ? "text-error font-semibold" : "font-semibold"}>
                        {r.status}
                      </span>
                      <span class="ml-2 opacity-80">{r.title}</span>
                      <span class="ml-2 opacity-60">{r.meta}</span>
                    </div>
                  </div>

                  <div class="flex items-center gap-3 text-xs opacity-60">
                    <span>{r.ago}</span>
                    <span class="h-2 w-2 rounded-full bg-base-content/40"></span>
                  </div>
                </div>
              {/each}
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</div>
