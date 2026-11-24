# The Slurm backend: developer notes

For setup and operation, see [Container Service on Slurm: Setup](../slurm/container-service-on-slurm-setup.md). This
page covers the code and how to test it.

## Where things are

| Class | Role |
|---|---|
| `model.server.docker.SlurmSettings` | Server-wide Slurm configuration, stored as `slurm_*` columns on `DockerServerEntity`. The token is write-only in JSON; `token-set` and `token-expires-at` are read-only. |
| `services.impl.DockerServerServiceImpl` | Keeps the stored token when a save omits it (only while the REST URL is unchanged), and drops Slurm settings for other backends. |
| `api.slurm.SlurmClient`, `SlurmClientImpl` | slurmrestd client: API version detection from `/openapi/v3`, submit, release, cancel, job lookup (falling back to slurmdbd), incremental job lists. |
| `api.slurm.SlurmClientFactoryImpl` | One cached client per distinct `SlurmSettings`. |
| `api.slurm.SlurmCredentialProvider`, `ServiceAccountCredentialProvider` | Where the user name and token come from. Per-user submission or automatic token renewal would be new providers. |
| `api.slurm.SlurmJobBuilder` | Turns a `Container` into a `SlurmJobRequest`: the bash job script that selects the image and runs Apptainer, plus resources. Per-command overrides belong here. |
| `api.slurm.SlurmJobPaths` | The job directory, `<build path>/slurm/<container id>`, as XNAT and the cluster see it. |
| `api.slurm.SlurmBackend` | Create, start, kill, remove and logs, called from `DockerControlApi` for `Backend.SLURM`. |
| `events.SlurmJobPoller` | Called by `ContainerStatusUpdater` on the primary node. Turns Slurm job states into `ServiceTaskEvent`s, which `ContainerServiceImpl.processEvent` handles as it does for Swarm. |

## Design notes

- **Lifecycle.** `create` submits the job held and stores the Slurm job ID as the container's service ID and task ID;
  `start` releases it. This mirrors Swarm's create-then-scale-up, so the existing launch flow needs no Slurm branch.
- **Polling.** Each tick makes one `GET /jobs?update_time=<cursor>` request. The cursor starts a minute before now
  and then follows Slurm's `last_update`; a response with `last_update` 0 means no change and leaves it alone. Every
  10 minutes the poller forgets what it knows and looks each job up again. Jobs it has not seen are looked up one by
  one, with slurmdbd as the fallback for purged jobs.
- **Exit codes.** The job script writes the container's exit status to `exit-code` atomically (`.tmp` then `mv`).
  The poller reads it only once a job is terminal, and prefers it to Slurm's exit code. If the file is missing, it uses
  Slurm's.
- **SIF paths.** `SlurmJobBuilder.sifPath` maps an image reference to a path under the SIF directory: each `/` a
  directory, the tag or digest the file name. Flattening the reference into one file name (replacing `/`, `:` and `@`
  with `_`) let different images share a file. Parts are checked so a reference cannot leave the SIF directory.
- **Cleanup.** `SlurmBackend.remove` deletes what it can of the job directory and throws if anything is left, so
  `ContainerServiceImpl.cleanupContainers` logs one warning naming it.
- **Live logs.** The history page polls `logSince` with a timestamp and appends what it gets. Slurm logs have no
  per-line timestamps, so for Slurm the "timestamp" carries the byte offset already read (epoch seconds = offset).
  `SlurmBackend.getLogFrom` returns at most 1 MiB of whole lines per call.
- **Apptainer arguments.** Jobs run `--no-eval`. Without it, an image's runscript passes arguments and
  `APPTAINERENV_*` values through `eval`. Environment variables reach the container only as `APPTAINERENV_*` (or
  `SINGULARITYENV_*`), because of `--cleanenv`.

## slurmrestd behaviour worth knowing

Observed on Slurm 23.11.4, API v0.0.40, and handled in `SlurmClientImpl`:

- The submit body needs the script at the top level (`{"script": ..., "job": {...}}`); `job.script` alone is
  rejected. v0.0.40 accepts both together, and the v0.0.43 docs describe the top-level field as the same script, so
  the client always sends both.
- Number fields such as `time_limit` and `memory_per_node` are `{set, infinite, number}` objects; `cpus_per_task` is
  a plain integer.
- An unknown job is HTTP 404 with error 2017 from slurmctld, but HTTP 200 with an empty `jobs` list and a warning from
  slurmdbd.
- Until a job ends, its exit code reads 0 (SUCCESS) and its end time is the start time plus the time limit. Ignore both.
- A held submit returns `result.error_code` 2042 (`ESLURM_JOB_HELD`) with warnings. It is not a failure.
- Signal names come back as `TERM`, not `SIGTERM`.
- Job IDs above about 67 million return HTTP 422 with error 9000.
- If slurmdbd does not accept JWTs, every request touching accounting fails with error 7000.

Recorded responses from a real server are in `src/test/resources/slurm/v0.0.40/`, with the hostname replaced by
`node01`. `SlurmClientRealResponsesTest` replays them.

## Running the tests

The Slurm unit tests need nothing special:

```bash
./gradlew test --tests 'org.nrg.containers.api.slurm.*' --tests 'org.nrg.containers.events.SlurmJobPollerTest'
```

The full suite includes tests that start Postgres and Docker with Testcontainers, among them `DockerServerEntityTest`,
which covers Slurm settings persistence. Without a Docker socket they fail with "Connection refused" rather than
skipping. With Rancher Desktop, for example:

```bash
DOCKER_HOST=unix://$HOME/.rd/docker.sock TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock ./gradlew test
```

### Live tests against a real cluster

`SlurmClientLiveTest`, `SlurmJobPollerLiveTest` and `SlurmBackendLiveTest` run small busybox jobs on a real cluster
and cancel or finish them. They are skipped unless these are set:

| Variable | Meaning |
|---|---|
| `SLURM_TEST_URL` | slurmrestd URL, e.g. `http://slurm.example.org:6820`. |
| `SLURM_TEST_TOKEN_FILE` | File holding the JWT, either bare or as `SLURM_JWT=<jwt>`. Keep it `chmod 600`. |
| `SLURM_TEST_USER` | Optional. Cluster account, default `xnat`. |
| `SLURM_TEST_PARTITION` | Optional. Partition to submit to. |

```bash
SLURM_TEST_URL=http://slurm.example.org:6820 SLURM_TEST_TOKEN_FILE=~/.slurm-test-token \
  ./gradlew test --tests '*SlurmClientLiveTest' --tests '*SlurmJobPollerLiveTest' --tests '*SlurmBackendLiveTest'
```

The compute nodes need Apptainer and access to Docker Hub. The tests' job directories go under `/tmp` on the node, so
they check the Slurm lifecycle, not log reading over a shared filesystem; they leave `/tmp/xnat-*` directories and a
`/tmp/xnat-live-test-cache` pull cache behind.
