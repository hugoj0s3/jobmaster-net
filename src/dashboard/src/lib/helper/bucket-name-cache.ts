import type { components } from "$lib/api/schema";

type ApiBucketModel = components["schemas"]["ApiBucketModel"];

class BucketNameCache {
    private readonly lookup = new Map<string, Map<string, string>>();

    get(clusterId: string, bucketId: string): string | undefined {
        return this.lookup.get(clusterId)?.get(bucketId);
    }

    getMissing(clusterId: string, bucketIds: string[]): string[] {
        const clusterMap = this.lookup.get(clusterId);
        if (!clusterMap) return bucketIds;
        return bucketIds.filter((id) => !clusterMap.has(id));
    }

    populate(clusterId: string, buckets: ApiBucketModel[]): void {
        if (!this.lookup.has(clusterId)) this.lookup.set(clusterId, new Map());
        const clusterMap = this.lookup.get(clusterId)!;
        for (const b of buckets) {
            if (b.id && b.name) clusterMap.set(b.id, b.name);
        }
    }
}

export const bucketNameCache = new BucketNameCache();