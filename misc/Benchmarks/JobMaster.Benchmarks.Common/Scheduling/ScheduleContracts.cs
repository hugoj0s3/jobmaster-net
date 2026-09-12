namespace JobMaster.Benchmarks.Common.Scheduling;

// Shared request/response wire contract between HttpScheduleClient (Runner side) and every
// framework host's /schedule-now and /schedule-after endpoints -- referencing this one shared type
// from both sides guarantees they can't drift apart independently.

public sealed record ScheduleNowRequest(int Count);

public sealed record ScheduleAfterRequest(int Count, double DelayMinutes);

public sealed record ScheduleResponse(List<Guid> JobIds);
