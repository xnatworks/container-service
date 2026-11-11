# Container Service on Slurm: Setup

This guide is for admins who want the Container Service to run containers as jobs on a Slurm cluster.

With the Slurm backend, XNAT submits each container as a Slurm batch job through
[slurmrestd](https://slurm.schedmd.com/rest.html), the Slurm REST API. On the compute node the job runs the command's
Docker image with [Apptainer](https://apptainer.org/) (or Singularity), and XNAT follows the job until it ends, then
finalizes the container as usual: it saves the logs and uploads the outputs.

- [How it works](#how-it-works)
- [Requirements](#requirements)
- [Cluster setup](#cluster-setup)
- [Shared filesystem and permissions](#shared-filesystem-and-permissions)
- [Configuring the container server in XNAT](#configuring-the-container-server-in-xnat)
- [Images: SIF files, registry pulls and air-gapped clusters](#images-sif-files-registry-pulls-and-air-gapped-clusters)
- [Testing the setup](#testing-the-setup)
- [Day-to-day operation](#day-to-day-operation)
- [Troubleshooting](#troubleshooting)
- [Limitations and planned work](#limitations-and-planned-work)

## How it works

When a user launches a container:

1. XNAT creates a **job directory** for it, `<build path>/slurm/<container id>`, under the XNAT build path.
2. XNAT submits a batch job through slurmrestd, **held**, as the cluster account configured on the container server.
   The job's name is `xnat-<container name>` and its comment is `XNAT container <id> launched by <user>`.
3. XNAT releases the hold, and Slurm schedules the job like any other.
4. On the compute node, the job script picks the image (a prebuilt SIF file if there is one, otherwise a pull from the
   image's registry) and runs it with `apptainer exec` or `apptainer run`. The command's mounts are bound into the
   container. Stdout and stderr go to `stdout.log` and `stderr.log` in the job directory, and the container's exit
   status is written to an `exit-code` file there.
5. XNAT's container status updater polls slurmrestd for job changes. When the job ends, XNAT reads the exit code,
   marks the container Complete or Failed, and finalizes it.
6. With auto-cleanup on, XNAT deletes the job directory once finalizing has saved the logs.

Containers can be killed from XNAT as usual; that cancels the Slurm job.

While a container runs, the history page shows its live stdout and stderr, read from the job directory.

## Requirements

**Slurm**
- Slurm with slurmrestd serving REST API version **v0.0.40 or newer** (Slurm 23.11 and later). XNAT detects which
  versions the server offers and uses the newest one it supports (v0.0.40 to v0.0.45). You can also pin a version.
- JWT authentication (`auth/jwt`) enabled for slurmctld, slurmrestd and, if you run accounting, slurmdbd.
- A cluster account for XNAT to submit jobs as, e.g. `xnat`, that exists on every compute node.

**Compute nodes**
- **Apptainer 1.1 or newer**, or **SingularityCE 3.10 or newer**. Jobs use `--no-eval`, which older versions lack.
  The runtime must be on the PATH of a login shell (job scripts start with `#!/bin/bash -l`, so a module system that
  loads it from the site profile works).
- Either access to the image registries your commands use (Docker Hub, etc.), or prebuilt SIF files. See
  [Images](#images-sif-files-registry-pulls-and-air-gapped-clusters).
- Network access to XNAT, if your commands call the XNAT API. Containers get the usual `XNAT_HOST`, `XNAT_USER` and
  `XNAT_PASS` variables; `XNAT_HOST` is the site's Processing URL if one is set, otherwise the site URL.

**XNAT**
- XNAT's archive and build directories on a filesystem the compute nodes also mount (NFS, GPFS, Lustre, etc.), as for
  every Container Service backend. See [Shared filesystem and permissions](#shared-filesystem-and-permissions).
- The site **build path** set in XNAT's site settings. Job directories live under it.
- Network access from XNAT to slurmrestd. On a multi-node XNAT, every node needs it: any node can launch or kill a
  container, and the status updater runs on whichever node holds the leader lock.

## Cluster setup

This section covers what the Container Service needs. For the general setup, see Slurm's
[REST API](https://slurm.schedmd.com/rest.html) and [JWT authentication](https://slurm.schedmd.com/jwt.html)
documentation.

### JWT authentication

slurmctld, slurmrestd and slurmdbd must all accept JWTs signed with the same key. In `slurm.conf`:

```
AuthAltTypes=auth/jwt
AuthAltParameters=jwt_key=/etc/slurm/jwt_hs256.key
```

**`slurmdbd.conf` needs the same two lines.** If slurmdbd lacks them, slurmctld still answers but every REST request
that touches accounting fails with error 7000 ("Authentication failure"), and slurmdbd logs that it does not recognize
the authentication type. Restart slurmdbd and slurmctld after the change.

Run slurmrestd with JWT authentication, e.g. `slurmrestd -a rest_auth/jwt 0.0.0.0:6820`, or the equivalent in your
service manager's configuration. Use TLS in front of slurmrestd (see below) for anything beyond a test cluster.

### The XNAT cluster account

XNAT submits every job as one cluster account. Create it on all nodes with the **same UID everywhere**, and make sure
it can:
- submit to the partition, account and QOS you configure in XNAT. If your cluster enforces associations
  (`AccountingStorageEnforce`), add the user with `sacctmgr`;
- write to the XNAT build directory and read the archive, as the compute nodes see them (see the next section);
- run Apptainer.

### The token

Create a token for that account on a host where you can run `scontrol` as root or as the SlurmUser:

```bash
scontrol token username=xnat lifespan=7776000   # 90 days, in seconds
```

It prints `SLURM_JWT=eyJ...`. Paste the part after `SLURM_JWT=` into XNAT.

**The default lifespan is 30 minutes**, so a token made without `lifespan` stops working almost at once. Pick a
lifespan that matches how often you are willing to replace it: XNAT shows the token's expiry in the container server
settings and warns from 7 days before it expires, but it cannot renew the token itself yet.

Anyone with the token can submit and cancel jobs as that account, so treat it like a password. XNAT never returns it
through its API or UI, but it is stored unencrypted in XNAT's database (the container server table), so access to
the database or its backups gives access to the token.

### TLS

Give XNAT an `https://` URL for anything beyond a test cluster, so the token is not sent in clear text. If your
slurmrestd does not serve TLS itself, put it behind a TLS-terminating proxy.

XNAT connects with Java's default trust store. If the proxy's certificate is signed by an internal CA, import that CA
into the trust store of the JVM that runs Tomcat (e.g. with `keytool -importcert`), then restart Tomcat.

## Shared filesystem and permissions

XNAT and the compute nodes exchange files through the shared filesystem: XNAT creates the job directory, the job
writes its logs and exit code there, and XNAT reads them back. The command's input and output mounts work the same
way, as on every backend.

### Path translation

If the compute nodes mount XNAT's data at a different path than XNAT does, set path translation on the container
server:

- **XNAT Path Prefix**: the shared directory as XNAT sees it, e.g. `/data/xnat`.
- **Server Path Prefix**: the same directory as the compute nodes see it, e.g. `/mnt/xnat`.

XNAT applies it to the job directory and to every mount, so the build path and the archive must both be under that
prefix. If both machines use the same paths, leave these blank.

The SIF directory and pull cache directory are not translated: enter them as the compute nodes see them.

### Permissions

XNAT creates the job directory, and the output directories for writable mounts, as the account Tomcat runs as. The
job then writes into them as the cluster account. Both accounts need write access to these directories:

- the simplest arrangement is for the cluster account to have the **same UID** as XNAT's Tomcat account;
- otherwise use a shared group, group-writable directories with the setgid bit, and a umask that keeps new files
  group-writable for both accounts.

If the cluster account cannot write to the job directory, the job fails without logs: Slurm cannot create
`stdout.log`. `scontrol show job <id>` and the slurmd log on the node show the reason.

### What a job directory holds

```
<build path>/slurm/<container id>/
├── stdout.log    the container's standard output
├── stderr.log    its standard error, and messages from the job script
├── exit-code     the container's exit status, written when it finishes
└── work/         Apptainer's scratch space for the container's writable layer
```

## Configuring the container server in XNAT

Go to **Administer > Plugin Settings > Container Service > Compute Backend**, edit the container server host (or
create one), and set **Type** to **Slurm**.

**Slurm Connection**

| Setting | Notes |
|---|---|
| slurmrestd URL | e.g. `https://slurm.example.org:6820`. |
| Slurm User | The cluster account jobs run as, e.g. `xnat`. |
| Token | The JWT for that account. The field is always empty, because XNAT never sends the token back; the text under it says whether one is stored and when it expires. Leave it blank to keep the stored token. You must enter it again if you change the URL: XNAT will not send a stored token to a different server. |
| REST API Version | Leave blank to detect it. Set it (e.g. `v0.0.40`) only to pin a version. |

**Job Settings** (applied to every job; leave blank for the cluster's defaults)

| Setting | Notes |
|---|---|
| Partition, Account, QOS | Passed to Slurm as given. |
| Time Limit | Any format `sbatch --time` accepts: `90` (minutes), `4:00:00`, `1-12` (days-hours), `UNLIMITED`. |

**Container Images**

| Setting | Notes |
|---|---|
| Container Runtime | `apptainer` or `singularity`: the command the job runs. |
| SIF Directory | Shared directory of prebuilt SIF files, as the compute nodes see it. |
| Pull images from registries? | On by default. Turn it off for clusters that cannot reach registries; a SIF directory is then required. |
| Pull Cache Directory | Shared Apptainer cache, as the compute nodes see it, so each pulled image is converted once instead of in every job. |

**Other settings** that apply to Slurm hosts as to other backends: path translation, automatic cleanup of completed
containers, automatic cleanup of build directories, and status emails.

After saving, the hosts table shows the ping status. Under it, a Slurm host shows **No token**, **Token expires soon**
(within 7 days) or **Token expired** when the token needs attention.

### Resources requested from Slurm

Each job asks for:

| Command setting | Slurm request |
|---|---|
| CPU limit | `cpus_per_task`, rounded up to a whole CPU. |
| Memory limit (or the reservation, if there is no limit) | Memory per node, in MB. Slurm has one memory figure for both. |
| `gpus`, or a generic resource whose name contains "gpu" | `gres/gpu:<count>`, and Apptainer's `--nv`. A non-numeric value such as `all` requests one GPU. |

These command settings are ignored, with a note in the log: ports, network, shared memory size, ulimits, runtime and
IPC mode. Swarm-style placement constraints are not offered for Slurm hosts.

## Images: SIF files, registry pulls and air-gapped clusters

Images in the XNAT UI work differently on Slurm, because there is no Docker daemon for XNAT to manage. The image list
is empty and the **New Image** button is hidden, as on Kubernetes. Add commands with **New Command** or by pasting the
command JSON. Image pulls happen on the compute nodes when a job starts.

For each job, the script looks for a SIF file for the command's image in the SIF directory. If it finds one, it runs
it. If not, it pulls `docker://<image>`, unless pulls are turned off, in which case the job fails at once with exit
code **125** and a message in stderr naming the SIF file it looked for.

### Where XNAT looks for a SIF

XNAT takes the image reference, removes Docker Hub's implicit `docker.io/` and `library/`, and adds `:latest` when
there is no tag or digest. Each `/` in the image name becomes a directory under the SIF directory, and the tag or
digest becomes the file name, plus `.sif`:

| Image in the command | SIF XNAT looks for, under the SIF directory |
|---|---|
| `busybox` | `busybox/latest.sif` |
| `xnat/dcm2niix:1.0.2` | `xnat/dcm2niix/1.0.2.sif` |
| `ghcr.io/org/tool:2.1` | `ghcr.io/org/tool/2.1.sif` |
| `registry.example.org:5000/lab/tool` | `registry.example.org:5000/lab/tool/latest.sif` |
| `lab/tool@sha256:4f3c...` | `lab/tool/sha256:4f3c....sif` |

Two different images never map to the same file. If an image has both a tag and a digest, the digest names the file.
A reference with a part that cannot be a file name, such as `..`, fails the launch while a SIF directory is set.

Build one on any host with Apptainer and registry access, then copy it into the SIF directory, keeping the
directories:

```bash
mkdir -p xnat/dcm2niix
apptainer build xnat/dcm2niix/1.0.2.sif docker://xnat/dcm2niix:1.0.2
```

Building from an image in a local Docker daemon also works: `apptainer build <path>.sif docker-daemon://<image>`.

### Air-gapped clusters

If the compute nodes cannot reach image registries:

1. Turn off **Pull images from registries?** and set the **SIF Directory**.
2. Build a SIF for each command's image on a connected host, at the path described above, and copy it to the SIF
   directory.
3. When you add or update a command, add its SIF too. A missing SIF fails the job quickly with exit code 125, rather
   than after a network timeout on the node.

If you have an internal registry mirror the nodes can reach, you can instead leave pulls on and point the commands'
images at the mirror.

### The pull cache

When pulls are on, jobs run with `APPTAINER_CACHEDIR` (or `SINGULARITY_CACHEDIR`) set to the pull cache directory.
Apptainer creates a `cache/` directory there, with downloaded layers under `blob/` and converted images under
`oci-tmp/`, so later jobs using the same image skip the download and conversion. Auto-cleanup does not touch the
cache, and it grows with each new image or tag. To prune it, run as the cluster account:

```bash
APPTAINER_CACHEDIR=/path/to/pull-cache apptainer cache clean --days 30
```

A removed image is downloaded again the next time a job needs it.

## Testing the setup

A command that logs for a while, writes to stderr and an output, and exits with a chosen code checks the whole path:

```json
{
    "name": "slurm-test",
    "description": "Checks Slurm logs, exit codes and outputs.",
    "type": "docker",
    "image": "busybox:latest",
    "command-line": "for i in $(seq 1 30); do echo \"stdout line $i of 30\"; sleep 2; done; echo 'a line on stderr' >&2; find /input -type f > /output/files.txt; exit #EXIT_CODE#",
    "override-entrypoint": true,
    "mounts": [
        {"name": "input", "writable": false, "path": "/input"},
        {"name": "output", "writable": true, "path": "/output"}
    ],
    "inputs": [
        {"name": "exit-code", "type": "string", "required": true, "default-value": "0",
         "user-settable": true, "replacement-key": "#EXIT_CODE#"}
    ],
    "outputs": [
        {"name": "files", "mount": "output", "required": true}
    ],
    "xnat": [
        {
            "name": "slurm-test",
            "contexts": ["xnat:projectData"],
            "external-inputs": [
                {"name": "project", "type": "Project", "required": true, "provides-files-for-command-mount": "input"}
            ],
            "output-handlers": [
                {"name": "files-handler", "accepts-command-output": "files", "as-a-child-of": "project",
                 "type": "Resource", "label": "SLURM_TEST"}
            ]
        }
    ]
}
```

Enable it for a project with a few files and launch it with exit code 0:

1. The container appears in the history and goes from Waiting to Running. `squeue` shows its job.
2. **View StdOut** shows "(live)" and a new line about every 2 seconds.
3. When it finishes, the status is **Complete**, the logs show "(from file)", StdErr has its line, and the project has
   a `SLURM_TEST` resource containing `files.txt`.

Launch it again with exit code 3: the status is **Failed**, with the message "Slurm job \<id> failed, exit code 3".

## Day-to-day operation

- **Statuses.** A job waiting in the queue shows as Waiting, then Running once it starts. The history message gives
  the Slurm job ID and, for a job that did not complete, why: its exit code, or that it reached its time limit, ran
  out of memory, lost its node, was preempted or was cancelled.
- **Held jobs.** XNAT submits each job held and releases it straight away. A job still on a user hold 15 minutes
  after it was submitted (for example because XNAT stopped before releasing it) is cancelled, and the container
  fails. An admin hold (`scontrol hold` run by root or an operator) is left alone: the container waits until the
  hold is released.
- **Purged jobs.** If Slurm has forgotten a job (after `MinJobAge`, and with no accounting record), XNAT decides the
  outcome from the job directory's `exit-code` file.
- **Exit codes on a shared filesystem.** XNAT trusts the `exit-code` file over Slurm's exit code, since it holds the
  container's own status. If the file is not visible yet when the job ends (NFS can cache file information for a few
  seconds), XNAT uses Slurm's exit code.
- **Cleanup.** With auto-cleanup on, XNAT deletes each job directory after finalizing. With it off, job directories
  stay in `<build path>/slurm/` until you remove them, as Docker containers stay when auto-cleanup is off. The
  scheduled build-directory cleanup does not remove them. If XNAT cannot delete everything in a job directory, it
  deletes what it can and logs a warning in `containers.log` naming the directory; remove the rest by hand. On NFS
  the usual cause is a `.nfs*` file that a process still holds open.
- **Token expiry.** Replace the token before it expires: create a new one with `scontrol token` and paste it into the
  container server settings. When it has expired, launches fail with an authentication error.
- **Logs.** The Slurm backend logs to XNAT's `containers.log`: each submission, release and cancellation, the
  slurmrestd API version in use, and warnings. Raise the `org.nrg.containers.api.slurm` logger to DEBUG to also see
  submission details and slurmrestd's own warnings. The token and job environments are never logged.

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| Every request fails with error 7000 ("Authentication failure") | slurmdbd does not accept JWTs. Add `AuthAltTypes` and `AuthAltParameters` to `slurmdbd.conf` and restart it. |
| Authentication errors after it used to work | The token has expired. Create a new one and save it in the container server settings. |
| Ping fails with "no slurmctld responded" | slurmrestd is up but cannot reach slurmctld. Check `scontrol ping` and the slurmctld service. |
| Saving the server fails with "Slurm token cannot be empty" | No token is stored, or the URL changed. Enter the token. |
| Launch fails: "The site build path is not configured" | Set the build path in XNAT's site settings. |
| Job fails at once with no logs | The cluster account cannot write the job directory, or the Server Path Prefix is wrong. Check the job's stdout path with `scontrol show job <id>`, and the permissions above. |
| Job fails with exit code 125 | No SIF for the image and pulls are off, or the pull failed. stderr names the SIF file it looked for. |
| Arguments arrive mangled, or `$0` is a script path | The runtime is too old for `--no-eval`. Upgrade to Apptainer 1.1+ or SingularityCE 3.10+. |
| `apptainer: command not found` in stderr | The runtime is not on the PATH of a login shell on the node. Load it from the site profile or install it system-wide. |
| Errors about `$HOME` | The cluster account has no passwd entry on the node. Jobs from slurmrestd have no HOME, and the script reads it from passwd. |
| Container stays Waiting | The job is pending. `squeue -j <id>` shows the reason (resources, priority, partition limits, etc.). |
| The live log omits the last line until the job ends | Expected: live logs show complete lines only. The saved log has everything. |
| An output is missing | Look in the output directory under the build path on XNAT. If the file is not there, the node wrote it somewhere XNAT cannot see; check path translation. |

## Limitations and planned work

- Every job runs as one cluster account. Submitting as the launching XNAT user is a possible future mode.
- Partition, account, QOS and time limit apply to every job. Per-command overrides (e.g. a longer limit or a GPU
  partition for one command) are planned.
- XNAT cannot renew the token yet, and stores it unencrypted. Both are planned.
- Building SIF files from XNAT (from Docker Hub, or from images on the XNAT server) is planned.
- slurmrestd API versions v0.0.41 to v0.0.45 are supported from Slurm's documentation and tests, but so far only
  v0.0.40 (Slurm 23.11) has been run against a real cluster.
