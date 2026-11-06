package org.nrg.containers.api.slurm;

import com.google.auto.value.AutoValue;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nullable;

/**
 * The per-job directory on the shared filesystem, as XNAT sees it and as the compute nodes see it. It holds the
 * job's stdout and stderr, the exit-code file and Apptainer's work directory. It must not be one of the
 * container's mounts, since output mounts are uploaded.
 */
@AutoValue
public abstract class SlurmJobPaths {
    public static final String STDOUT = "stdout.log";
    public static final String STDERR = "stderr.log";

    /** Written by the batch script when the container exits, so the result survives slurmctld purging the job. */
    public static final String EXIT_CODE = "exit-code";

    /** Apptainer's {@code --workdir}: backs the container's /tmp and /var/tmp. */
    public static final String WORK = "work";

    public abstract String xnatPath();
    public abstract String clusterPath();

    public static SlurmJobPaths create(final String xnatPath, final String clusterPath) {
        return new AutoValue_SlurmJobPaths(strip(xnatPath), strip(clusterPath));
    }

    /**
     * Uses the server's path translation, the same way mounts are translated: a path under {@code xnatPrefix} is
     * moved under {@code clusterPrefix}. Without both prefixes both sides share the path.
     */
    public static SlurmJobPaths translate(final String xnatPath, @Nullable final String xnatPrefix, @Nullable final String clusterPrefix) {
        final String stripped = strip(xnatPath);
        if (StringUtils.isAnyBlank(xnatPrefix, clusterPrefix)) {
            return create(stripped, stripped);
        }
        final String from = strip(xnatPrefix);
        if (!stripped.equals(from) && !stripped.startsWith(from + "/")) {
            return create(stripped, stripped);
        }
        return create(stripped, strip(clusterPrefix) + stripped.substring(from.length()));
    }

    public String xnatFile(final String name) {
        return xnatPath() + "/" + name;
    }

    public String clusterFile(final String name) {
        return clusterPath() + "/" + name;
    }

    private static String strip(final String path) {
        final String stripped = StringUtils.stripEnd(StringUtils.trimToEmpty(path), "/");
        return stripped.isEmpty() ? "/" : stripped;
    }
}
