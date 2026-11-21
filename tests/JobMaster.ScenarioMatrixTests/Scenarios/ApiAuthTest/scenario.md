# ApiAuthTest

Under each API auth mode, the API lists every registered cluster, and a job scheduled on each cluster's own container
is readable through it. Matrix port of `JobMaster.ScenarioTests/Scenarios/ApiAuths` (one copy per auth mode there:
NoAuth, UserPass, ApiKey, Jwt).

## Variants
The axis is the **auth mode**: `noauth`, `userpass`, `apikey`, `jwt`, filled into the api container's `auth` block
(`{{authConfig}}`; the runner's per-run API key, username and password are attached to `Runner.Api` automatically;
for `jwt` the test fetches a bearer token first). Flavor: PostgresPure only, like the regular suite: auth is in the
API layer, the storage provider doesn't change it. (Open decision in Plan item 23: switch to `AllExcept(...)` to
multiply by provider.)

## Topology
Two standalone clusters `mx-<flavor>-<auth>-one` / `-two`, each in its own container named after it, **sharing one
master database** (`ApiAuth<auth>`); the `api` container sees both.

## Flow
1. The API's cluster list contains both clusters.
2. Per cluster: schedule one `fast` job on its own container (the proxy round-robins across containers, which would
   misroute between distinct clusters), it runs (30 s max), and the API returns it.
