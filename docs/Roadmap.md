# 🗺️ JobMaster Roadmap

The following roadmap outlines the strategic direction of JobMaster, focusing on developer control, operational visibility, and management flexibility.

---

---

## 🖥️ Phase 1: The Dashboard (Visibility)

### JobMaster Dashboard
UI built on top of JobMaster API.

#### Cluster Monitoring
Visual tracking of Cluster health, Agent transport status, and Worker heartbeats.

#### Audit & Logs
Decoupling the Job from the Execution. This allows tracking multiple execution attempts of the same job (reschedules, retries, etc.), with dedicated logs for each individual execution attempt.

## 🛠️ Phase 2: Decoupling & Configuration

### Job Definition Separation
Implement a mechanism to fully decouple Job Definitions (metadata, default priorities, timeouts) from the Job Handler (the actual business logic code). It will be as optional, we can keep the existing hanler.

### Scheduling from lambda expression
Scheduling from lambda expression provide it as optional package, make easier migration from some existing frameworks.
---

## ⚡ Phase 3: Dynamic Operations (API & Dashboard)
### Dynamic Scheduling
- **Ad-hoc Scheduling**: Capability to schedule one-off or recurring jobs directly through the API or Dashboard without new deployments.

- **Job Rescheduling**: Real-time management to modify the execution time, priority, or data of "Pending" or "In-flight" jobs.

- **Manual Triggering**: "Run Now" support for recurring schedules to facilitate manual testing and urgent ad-hoc executions.

### Remote Worker Control
- **Graceful Shutdown**: Ability to request workers to stop or pause via the API/Dashboard, ensuring they finish current tasks before going offline.

-- **Host Telemetry**: Native integration for Container/Host metrics (CPU, Memory, Disk I/O) to identify bottlenecks at the execution layer.


---

## 📋 Timeline & Priorities

| Phase | Focus | Priority | Status |
|-------|-------|----------|--------|
| Phase 1 | Dashboard & Visibility | High | 🕒 Planned |
| Phase 2 | Decoupling & Configuration | Medium | 🕒 Planned |
| Phase 3 | Dynamic Operations | Low | 🕒 Planned |
---

## 🤝 Contributing

We welcome community contributions! If you're interested in helping with any of these initiatives:

1. **Check the [Issues](../../issues)** for related tickets
2. **Join discussions** in existing roadmap issues
3. **Create proposals** for new features following our contribution guidelines

---

## 📝 Notes

- This roadmap is iterative and may evolve based on community feedback and emerging needs
- Each phase builds upon the previous one to ensure a solid foundation
- Priority may shift based on user demand and technical dependencies