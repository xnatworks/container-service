package org.nrg.containers.api.slurm;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.nrg.containers.exceptions.ContainerException;
import org.nrg.containers.model.container.auto.Container;
import org.nrg.containers.model.container.auto.Container.ContainerMount;
import org.nrg.containers.model.server.docker.SlurmSettings;
import org.nrg.containers.secrets.ContainerPropertiesWithSecretValues;
import org.nrg.containers.utils.ShellSplitter;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;

/**
 * Turns a {@link Container} into a Slurm batch job that runs its image with Apptainer (or Singularity).
 * <p>
 * The command runs as it would under Docker:
 * <ul>
 *     <li>with {@code override-entrypoint}, {@code <runtime> exec IMAGE /bin/sh -c '<command-line>'}, matching
 *     Docker's {@code /bin/sh -c} with the entrypoint cleared;</li>
 *     <li>otherwise {@code <runtime> run IMAGE <args>}, where {@code run} keeps the image's ENTRYPOINT and the
 *     arguments replace its CMD, matching Docker. Arguments are split the same way as for Swarm and each one is
 *     quoted.</li>
 * </ul>
 * Both run with {@code --no-eval}, so arguments and environment values reach the container exactly as given, as
 * under Docker. That needs Apptainer 1.1 or later, or SingularityCE 3.10 or later.
 * The image is chosen on the compute node when the job starts: a SIF in {@link SlurmSettings#sifDirectory()} at
 * {@link #sifPath(String)} if present, otherwise {@code docker://} (unless registry pulls are disabled, in which
 * case the job exits with {@value #EXIT_IMAGE_UNAVAILABLE}). XNAT therefore never needs to see the SIF directory.
 * <p>
 * Environment variables and secrets go into the job's environment with the runtime's {@code ENV_} prefix, never
 * into the script. Slurm keeps a job's environment where cluster admins and the submitting account can read it.
 */
@Slf4j
public final class SlurmJobBuilder {
    /** Exit code when the image is not available, matching Docker's 125 for failures before the container runs. */
    public static final int EXIT_IMAGE_UNAVAILABLE = 125;

    private static final Pattern ENV_NAME = Pattern.compile("^[A-Za-z_][A-Za-z0-9_]*$");
    private static final Pattern GPU_COUNT = Pattern.compile("^\\d+$");
    /** A host (with port), repository part, tag or digest. The first character keeps out "", "." and "..". */
    private static final Pattern SIF_PATH_COMPONENT = Pattern.compile("^[A-Za-z0-9_][A-Za-z0-9._:+-]*$");

    private SlurmJobBuilder() {
    }

    public static SlurmJobRequest build(final Container container, final SlurmSettings settings, final SlurmJobPaths paths)
            throws ContainerException {
        final String runtime = settings.runtime();
        final String envPrefix = runtime.toUpperCase(Locale.ROOT) + "ENV_";
        final Integer gpus = gpuCount(container);

        final SlurmJobRequest.Builder builder = SlurmJobRequest.builder(settings)
                .name(jobName(container))
                .comment(comment(container))
                .script(script(container, settings, paths, gpus != null))
                .workingDirectory(paths.clusterPath())
                .standardOutput(paths.clusterFile(SlurmJobPaths.STDOUT))
                .standardError(paths.clusterFile(SlurmJobPaths.STDERR));

        builder.environmentBuilder().add(SlurmJobRequest.DEFAULT_PATH);
        // Sorted so the job's environment does not depend on map order.
        final Map<String, String> environment = new TreeMap<>(ContainerPropertiesWithSecretValues.prepareSecretsForLaunch(container).environmentVariables());
        for (final Map.Entry<String, String> variable : environment.entrySet()) {
            if (!ENV_NAME.matcher(variable.getKey()).matches()) {
                throw new ContainerException("Environment variable name \"" + variable.getKey() + "\" cannot be passed to " + runtime);
            }
            builder.addEnvironment(envPrefix + variable.getKey(), StringUtils.defaultString(variable.getValue()));
        }

        if (container.limitCpu() != null && container.limitCpu() > 0) {
            builder.cpusPerTask((int) Math.ceil(container.limitCpu()));
        }
        // Slurm has one memory figure, both reservation and limit; Docker's limit is the closer match.
        final Long memory = positive(container.limitMemory()) != null ? container.limitMemory() : positive(container.reserveMemory());
        if (memory != null) {
            builder.memoryPerNodeMb(memory);
        }
        if (gpus != null) {
            builder.tresPerNode("gres/gpu:" + gpus);
        }

        logIgnoredSettings(container);
        return builder.build();
    }

    /**
     * Where XNAT looks for an image's SIF, relative to the SIF directory: the image reference with Docker Hub's implicit
     * {@code docker.io/} and {@code library/} removed and {@code :latest} added when there is no tag or digest. Each
     * {@code /} in the name becomes a directory and the tag or digest becomes the file name, so
     * {@code xnat/dcm2niix:1.0.2} is {@code xnat/dcm2niix/1.0.2.sif}, {@code ubuntu} is {@code ubuntu/latest.sif} and
     * {@code lab/tool@sha256:abc} is {@code lab/tool/sha256:abc.sif}. Distinct references never share a path: a tag
     * cannot contain {@code :} and a digest always does. With both a tag and a digest, the digest names the file.
     *
     * @throws IllegalArgumentException if a part of the reference could not be a file name, such as {@code ..}
     */
    public static String sifPath(final String image) {
        String name = StringUtils.trimToEmpty(image);
        name = StringUtils.removeStart(name, "docker://");
        name = StringUtils.removeStart(name, "docker.io/");
        name = StringUtils.removeStart(name, "index.docker.io/");
        name = StringUtils.removeStart(name, "library/");

        final String version;
        final int at = name.indexOf('@');
        if (at >= 0) {
            version = name.substring(at + 1);
            name = name.substring(0, at);
            final int tag = name.lastIndexOf(':');
            if (tag > name.lastIndexOf('/')) {
                name = name.substring(0, tag);
            }
        } else {
            final int tag = name.lastIndexOf(':');
            if (tag > name.lastIndexOf('/')) {
                version = name.substring(tag + 1);
                name = name.substring(0, tag);
            } else {
                version = "latest";
            }
        }

        final String path = name + "/" + version;
        for (final String component : path.split("/", -1)) {
            if (!SIF_PATH_COMPONENT.matcher(component).matches()) {
                throw new IllegalArgumentException("\"" + image + "\" is not an image reference XNAT can look up in the SIF directory");
            }
        }
        return path + ".sif";
    }

    static String script(final Container container, final SlurmSettings settings, final SlurmJobPaths paths, final boolean gpus)
            throws ContainerException {
        final String runtime = settings.runtime();
        final StringBuilder script = new StringBuilder();
        // A login shell picks up the site profile, which is where module systems put apptainer on the PATH.
        script.append("#!/bin/bash -l\n");
        script.append("# XNAT container ").append(container.databaseId())
                .append(", command ").append(container.commandId())
                .append(", wrapper ").append(container.wrapperId())
                .append(", launched by ").append(sanitizeComment(container.userId())).append('\n');
        script.append("set -u\n");
        script.append("JOB_DIR=").append(quote(paths.clusterPath())).append('\n');
        script.append("EXIT_FILE=\"$JOB_DIR/").append(SlurmJobPaths.EXIT_CODE).append("\"\n");
        script.append("rm -f \"$EXIT_FILE\"\n");
        script.append("mkdir -p \"$JOB_DIR/").append(SlurmJobPaths.WORK).append("\"\n");
        // Jobs submitted through slurmrestd get no HOME, and apptainer needs one.
        script.append("export HOME=\"${HOME:-$(getent passwd \"$(id -un)\" | cut -d: -f6)}\"\n");
        script.append("finish() { echo \"$1\" > \"$EXIT_FILE.tmp\" && mv -f \"$EXIT_FILE.tmp\" \"$EXIT_FILE\"; exit \"$1\"; }\n");
        script.append('\n');

        appendImageSelection(script, container.dockerImage(), settings);

        script.append(runtime).append(container.overrideEntrypointNonnull() ? " exec" : " run");
        // --no-eval: without it the image's runscript passes the arguments, and env values, through eval, so $,
        // quotes and backticks in them are expanded a second time. Needs Apptainer 1.1+ or SingularityCE 3.10+.
        script.append(" --no-eval --cleanenv --contain --writable-tmpfs --workdir \"$JOB_DIR/").append(SlurmJobPaths.WORK).append('"');
        if (gpus) {
            script.append(" --nv");
        }
        if (StringUtils.isNotBlank(container.workingDirectory())) {
            script.append(" --pwd ").append(quote(container.workingDirectory()));
        }
        for (final ContainerMount mount : container.mounts()) {
            script.append(" \\\n    --bind ").append(quote(bind(mount)));
        }
        script.append(" \\\n    \"$IMAGE\"");
        if (container.overrideEntrypointNonnull()) {
            script.append(" /bin/sh -c ").append(quote(container.commandLine()));
        } else if (StringUtils.isNotBlank(container.commandLine())) {
            for (final String argument : ShellSplitter.shellSplit(container.commandLine())) {
                script.append(' ').append(quote(argument));
            }
        }
        script.append('\n');
        script.append("finish $?\n");
        return script.toString();
    }

    private static void appendImageSelection(final StringBuilder script, final String image, final SlurmSettings settings)
            throws ContainerException {
        final String dockerRef = image.startsWith("docker://") ? image : "docker://" + image;
        final String runtimeVar = settings.runtime().toUpperCase(Locale.ROOT);
        final boolean pull = settings.allowDockerPull();

        if (pull && StringUtils.isNotBlank(settings.pullCacheDirectory())) {
            script.append("export ").append(runtimeVar).append("_CACHEDIR=").append(quote(settings.pullCacheDirectory())).append('\n');
        }
        if (StringUtils.isBlank(settings.sifDirectory())) {
            script.append("IMAGE=").append(quote(dockerRef)).append("\n\n");
            return;
        }
        final String sif;
        try {
            sif = StringUtils.stripEnd(settings.sifDirectory(), "/") + "/" + sifPath(image);
        } catch (IllegalArgumentException e) {
            throw new ContainerException(e.getMessage());
        }
        script.append("SIF=").append(quote(sif)).append('\n');
        script.append("if [ -f \"$SIF\" ]; then\n");
        script.append("    IMAGE=\"$SIF\"\n");
        script.append("else\n");
        if (pull) {
            script.append("    IMAGE=").append(quote(dockerRef)).append('\n');
        } else {
            script.append("    echo ").append(quote("XNAT: image " + image + " is not available: " + sif
                    + " does not exist and registry pulls are disabled.")).append(" >&2\n");
            script.append("    finish ").append(EXIT_IMAGE_UNAVAILABLE).append('\n');
        }
        script.append("fi\n\n");
    }

    /** Apptainer bind syntax is {@code src:dest[:ro]}, comma-separated, so neither path may contain : or ,. */
    private static String bind(final ContainerMount mount) throws ContainerException {
        final String source = mount.containerHostPath();
        final String destination = mount.containerPath();
        for (final String path : new String[]{source, destination}) {
            if (StringUtils.isBlank(path) || StringUtils.containsAny(path, ':', ',')) {
                throw new ContainerException("Mount \"" + mount.name() + "\" has path \"" + path
                        + "\", which apptainer cannot bind: paths must be set and must not contain ':' or ','");
            }
        }
        return source + ":" + destination + (mount.writable() ? "" : ":ro");
    }

    /**
     * GPUs requested by the command: its {@code gpus} setting, or a generic resource whose name contains "gpu".
     * Slurm needs a count, so {@code all} (or any other non-numeric value) requests one.
     *
     * @return the GPU count, or null when the command asks for none
     */
    static Integer gpuCount(final Container container) {
        final String gpus = StringUtils.trimToNull(container.gpus());
        if (gpus != null) {
            if (GPU_COUNT.matcher(gpus).matches()) {
                final int count = Integer.parseInt(gpus);
                return count > 0 ? count : null;
            }
            log.info("Container {} asks for gpus \"{}\"; Slurm needs a count, so requesting 1", container.databaseId(), gpus);
            return 1;
        }
        if (container.genericResources() != null) {
            for (final Map.Entry<String, String> resource : container.genericResources().entrySet()) {
                if (resource.getKey().toLowerCase(Locale.ROOT).contains("gpu")) {
                    final String value = StringUtils.trimToEmpty(resource.getValue());
                    return GPU_COUNT.matcher(value).matches() && Integer.parseInt(value) > 0 ? Integer.valueOf(value) : 1;
                }
            }
        }
        return null;
    }

    private static void logIgnoredSettings(final Container container) {
        final List<String> ignored = new ArrayList<>();
        if (!container.ports().isEmpty()) {
            ignored.add("ports");
        }
        if (StringUtils.isNotBlank(container.network())) {
            ignored.add("network");
        }
        if (container.shmSize() != null) {
            ignored.add("shm-size");
        }
        if (container.ulimits() != null && !container.ulimits().isEmpty()) {
            ignored.add("ulimits");
        }
        if (StringUtils.isNotBlank(container.runtime())) {
            ignored.add("runtime");
        }
        if (StringUtils.isNotBlank(container.ipcMode())) {
            ignored.add("ipc-mode");
        }
        if (!ignored.isEmpty()) {
            log.info("Container {} sets {}, which the Slurm backend does not support; ignoring", container.databaseId(), ignored);
        }
    }

    private static String jobName(final Container container) {
        final String name = StringUtils.defaultIfBlank(container.containerName(), String.valueOf(container.databaseId()));
        return "xnat-" + name.replaceAll("[^A-Za-z0-9._-]", "_");
    }

    private static String comment(final Container container) {
        return "XNAT container " + container.databaseId() + " launched by " + sanitizeComment(container.userId());
    }

    private static String sanitizeComment(final String text) {
        return StringUtils.defaultString(text).replaceAll("[^A-Za-z0-9@._-]", "_");
    }

    private static Long positive(final Long value) {
        return value != null && value > 0 ? value : null;
    }

    /** Quotes a value for bash: single quotes, with embedded single quotes closed, escaped and reopened. */
    static String quote(final String value) {
        return "'" + StringUtils.defaultString(value).replace("'", "'\\''") + "'";
    }
}
