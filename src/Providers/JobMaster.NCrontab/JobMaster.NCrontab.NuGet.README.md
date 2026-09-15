> [!WARNING]
> **Experimental Alpha Release**
>
> This package is in an early stage and subject to significant changes before 1.0.
> Features and APIs may evolve, and stability is not guaranteed. Not recommended for production environments.

# JobMaster.NCrontab
### Standard cron expression support for JobMaster .Net, powered by [NCrontab](https://github.com/atifaziz/NCrontab).

This package adds an `NCrontab` recurrence provider for **JobMaster .Net** recurring schedules, letting you use familiar 5-field (`* * * * *`) or 6-field-with-seconds (`* * * * * *`) cron syntax instead of `TimeSpan` intervals or `NaturalCron` expressions.

## 📦 Installation

Install the package via the .NET CLI:

```bash
dotnet add package JobMaster
dotnet add package JobMaster.NCrontab
```

## 🚀 Getting Started

Register the NCrontab compiler once at startup, before `AddJobMasterCluster`:

```csharp
using JobMaster.NCrontab;

builder.Services.AddJobMasterNCrontab();
builder.Services.AddJobMasterCluster(config => { ... });
```

### Dynamic schedules

```csharp
using JobMaster.NCrontab;

await scheduler.RecurringAsync<MyHandler>("*/5 * * * *"); // every 5 minutes
```

### Attribute-based static schedules

```csharp
using JobMaster.NCrontab;

[NCrontabSchedule("0 18 * * 1-5")] // 18:00 on weekdays
public sealed class ReportHandler : IJobMasterHandler
{
    public async Task HandleAsync(JobContext job) { ... }
}
```

## 🛠 Features
* **Standard cron syntax:** 5-field and 6-field-with-seconds formats, auto-detected from the expression.
* **Drop-in provider:** works alongside `TimeSpanInterval`/`NaturalCron` — pick whichever fits each schedule.

---
**Main Project:** [JobMaster .Net](https://github.com/hugoj0s3/jobmaster-net)
**Docs:** [Recurring Schedule guide](https://docs.jobmaster.hugoj0s3.dev/docs/scheduling/recurring-schedule)
**License:** MIT
