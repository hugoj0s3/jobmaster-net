using JobMaster.Api.ApiModels;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Routing;
using System;
using System.Collections.Generic;
using System.Linq;

namespace JobMaster.Api.Endpoints;

internal static class HostsEndpoints
{
    internal static RouteGroupBuilder MapHostsEndpoints(this RouteGroupBuilder group)
    {
        var hosts = group.GetClusterEntityGroup("hosts");

        // TODO: This is a mock endpoint (randomized values) until host telemetry is implemented.

        hosts.MapGet("/", ListHostsAsync);
        hosts.MapGet("/count", CountHostsAsync);
        hosts.MapGet("/{hostId}", GetHostAsync);

        return group;
    }

    private static Task<IResult> CountHostsAsync(
        [FromRoute] string clusterId,
        CancellationToken ct)
    {
        var count = GetMockHosts(clusterId).Count;
        return Task.FromResult(Results.Ok(count));
    }

    private static List<ApiHostModel> GetMockHosts(string clusterId)
    {
        var seed = clusterId?.GetHashCode(StringComparison.Ordinal) ?? 0;
        var rng = new Random(seed);

        var result = new List<ApiHostModel>();
        var count = rng.Next(4, 9);

        for (var i = 1; i <= count; i++)
        {
            var totalGb = rng.Next(8, 257);
            var totalBytes = totalGb * 1024L * 1024 * 1024;

            var usedPercent = rng.NextDouble() * 0.85;
            var usedBytes = (long)(totalBytes * usedPercent);

            result.Add(new ApiHostModel
            {
                Id = $"host-{i}",
                DisplayName = $"Host {i}",
                CpuUsagePercent = Math.Round(rng.NextDouble() * 100, 2),
                MemoryTotalBytes = totalBytes,
                MemoryUsedBytes = usedBytes,
                ThreadCount = rng.Next(20, 500),
                HandleCount = rng.Next(200, 5000)
            });
        }

        return result;
    }

    private static Task<IResult> ListHostsAsync(
        [FromRoute] string clusterId,
        CancellationToken ct)
    {
        var result = GetMockHosts(clusterId);
        return Task.FromResult(Results.Ok(result));
    }

    private static Task<IResult> GetHostAsync(
        [FromRoute] string clusterId,
        [FromRoute] string hostId,
        CancellationToken ct)
    {
        var list = GetMockHosts(clusterId);
        var item = list.FirstOrDefault(x => string.Equals(x.Id, hostId, StringComparison.OrdinalIgnoreCase));
        return Task.FromResult(item == null ? Results.NotFound() : Results.Ok(item));
    }
}
