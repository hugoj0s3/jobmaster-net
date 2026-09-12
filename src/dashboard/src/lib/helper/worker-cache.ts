import type { components } from "$lib/api/schema";

type ApiAgentWorker = components["schemas"]["ApiAgentWorker"];

export type WorkerCacheEntry = {
    name: string;
    mode: number | null;
};

class WorkerCache {
    private readonly lookup = new Map<string, Map<string, WorkerCacheEntry>>();

    get(clusterId: string, workerId: string): WorkerCacheEntry | undefined {
        return this.lookup.get(clusterId)?.get(workerId);
    }

    getMissing(clusterId: string, workerIds: string[]): string[] {
        const clusterMap = this.lookup.get(clusterId);
        if (!clusterMap) return workerIds;
        return workerIds.filter((id) => !clusterMap.has(id));
    }

    populate(clusterId: string, workers: ApiAgentWorker[]): void {
        if (!this.lookup.has(clusterId)) this.lookup.set(clusterId, new Map());
        const clusterMap = this.lookup.get(clusterId)!;
        for (const w of workers) {
            if (w.id) {
                clusterMap.set(w.id, {
                    name: w.name ?? w.id,
                    mode: w.mode ?? null
                });
            }
        }
    }
}

export const workerCache = new WorkerCache();