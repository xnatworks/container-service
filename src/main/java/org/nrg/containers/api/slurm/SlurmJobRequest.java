package org.nrg.containers.api.slurm;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import org.nrg.containers.model.server.docker.SlurmSettings;

import javax.annotation.Nullable;
import java.util.stream.Collectors;

/**
 * A batch job to submit through slurmrestd. The client turns this into the version-specific request body, so
 * nothing outside the client depends on the slurmrestd JSON layout.
 */
@AutoValue
public abstract class SlurmJobRequest {
    /** Used when the caller sets no environment; slurmctld rejects batch jobs with an empty environment. */
    static final String DEFAULT_PATH = "PATH=/usr/local/bin:/usr/bin:/bin";

    public abstract String name();

    /** The batch script, starting with a {@code #!} line. */
    public abstract String script();

    /** Directory on the cluster's shared filesystem the job starts in. */
    public abstract String workingDirectory();

    /** Path for stdout. Slurm replaces {@code %j} with the job ID. */
    public abstract String standardOutput();

    /** Path for stderr; null sends stderr to the stdout file. */
    @Nullable public abstract String standardError();

    /**
     * {@code NAME=value} entries for the job's environment. Slurm stores these with the job, where cluster admins
     * and the submitting account can read them, so secrets here are only as private as that account.
     */
    public abstract ImmutableList<String> environment();

    @Nullable public abstract String partition();
    @Nullable public abstract String account();
    @Nullable public abstract String qos();
    @Nullable public abstract Integer timeLimitMinutes();
    @Nullable public abstract Integer cpusPerTask();
    @Nullable public abstract Long memoryPerNodeMb();

    /** Generic resources per node, e.g. {@code gres/gpu:1}. */
    @Nullable public abstract String tresPerNode();

    /** Submit held, so the job waits for {@link SlurmClient#release(long)}. */
    public abstract boolean hold();

    @Nullable public abstract String comment();

    public static Builder builder() {
        return new AutoValue_SlurmJobRequest.Builder().hold(true);
    }

    /** A builder with the server-wide partition, account, QOS and time limit already applied. */
    public static Builder builder(final SlurmSettings settings) {
        return builder()
                .partition(settings.partition())
                .account(settings.account())
                .qos(settings.qos())
                .timeLimitMinutes(settings.timeLimitMinutes());
    }

    public abstract Builder toBuilder();

    /** Leaves out environment values and the script, either of which may hold secrets. */
    @Override
    public String toString() {
        return "SlurmJobRequest{name=" + name()
                + ", workingDirectory=" + workingDirectory()
                + ", standardOutput=" + standardOutput()
                + ", standardError=" + standardError()
                + ", environment=" + environment().stream().map(e -> e.split("=", 2)[0]).collect(Collectors.joining(",", "[", "]"))
                + ", script=" + script().length() + " chars"
                + ", partition=" + partition()
                + ", account=" + account()
                + ", qos=" + qos()
                + ", timeLimitMinutes=" + timeLimitMinutes()
                + ", cpusPerTask=" + cpusPerTask()
                + ", memoryPerNodeMb=" + memoryPerNodeMb()
                + ", tresPerNode=" + tresPerNode()
                + ", hold=" + hold()
                + "}";
    }

    @AutoValue.Builder
    public abstract static class Builder {
        public abstract Builder name(String name);
        public abstract Builder script(String script);
        public abstract Builder workingDirectory(String workingDirectory);
        public abstract Builder standardOutput(String standardOutput);
        public abstract Builder standardError(String standardError);
        public abstract Builder environment(Iterable<String> environment);
        public abstract ImmutableList.Builder<String> environmentBuilder();
        public abstract Builder partition(String partition);
        public abstract Builder account(String account);
        public abstract Builder qos(String qos);
        public abstract Builder timeLimitMinutes(Integer timeLimitMinutes);
        public abstract Builder cpusPerTask(Integer cpusPerTask);
        public abstract Builder memoryPerNodeMb(Long memoryPerNodeMb);
        public abstract Builder tresPerNode(String tresPerNode);
        public abstract Builder hold(boolean hold);
        public abstract Builder comment(String comment);

        public Builder addEnvironment(final String name, final String value) {
            environmentBuilder().add(name + "=" + value);
            return this;
        }

        abstract SlurmJobRequest autoBuild();

        public SlurmJobRequest build() {
            final SlurmJobRequest request = autoBuild();
            if (!request.script().startsWith("#!")) {
                throw new IllegalArgumentException("A Slurm batch script must start with a #! line");
            }
            return request;
        }
    }
}
