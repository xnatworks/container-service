package org.nrg.containers.api.slurm;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * A job's state as reported by slurmctld or, after slurmctld has purged the job, by slurmdbd.
 * <p>
 * Slurm reports a job state as one base state plus flags, e.g. {@code [COMPLETED, COMPLETING]} while the epilog
 * still runs, or {@code [PENDING, REQUEUED]}. {@link #phase()} depends only on the base state.
 */
@AutoValue
public abstract class SlurmJobState {
    public enum Source {
        /** slurmctld, which knows the job until {@code MinJobAge} after it ends. */
        CONTROLLER,
        /** slurmdbd accounting, which keeps finished jobs after slurmctld purges them. */
        ACCOUNTING
    }

    public enum Phase {
        PENDING, RUNNING, SUCCEEDED, FAILED, CANCELLED, UNKNOWN;

        public boolean isTerminal() {
            return this == SUCCEEDED || this == FAILED || this == CANCELLED;
        }
    }

    static final Set<String> BASE_STATES = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            "PENDING", "RUNNING", "SUSPENDED", "COMPLETED", "CANCELLED", "FAILED", "TIMEOUT", "NODE_FAIL",
            "PREEMPTED", "BOOT_FAIL", "DEADLINE", "OUT_OF_MEMORY")));

    public abstract long jobId();

    /** The base state, e.g. {@code RUNNING}, or {@code UNKNOWN} if Slurm sent none this client recognizes. */
    public abstract String state();

    /** State flags such as {@code COMPLETING}, {@code REQUEUED} or {@code CONFIGURING}. */
    public abstract ImmutableSet<String> flags();

    /** Why the job is pending or ended, e.g. {@code JobHeldUser} or {@code NonZeroExitCode}. */
    @Nullable public abstract String reason();

    /** True while the job waits for {@link SlurmClient#release(long)} (or an admin release). */
    public abstract boolean held();

    /** The batch script's exit code, once it has ended. */
    @Nullable public abstract Integer exitCode();

    /** Name of the signal that ended the job, if a signal did. */
    @Nullable public abstract String signal();

    /** Nodes the job runs or ran on, in Slurm's compressed form, e.g. {@code node[01-04]}. */
    @Nullable public abstract String nodes();

    /** Start and end times as epoch seconds. */
    @Nullable public abstract Long startTime();
    @Nullable public abstract Long endTime();

    public abstract Source source();

    public static Builder builder() {
        return new AutoValue_SlurmJobState.Builder().flags(ImmutableSet.of()).held(false).state("UNKNOWN");
    }

    public abstract Builder toBuilder();

    public Phase phase() {
        switch (state()) {
            case "PENDING":
                return Phase.PENDING;
            case "RUNNING":
            case "SUSPENDED":
                return Phase.RUNNING;
            case "COMPLETED":
                return Phase.SUCCEEDED;
            case "CANCELLED":
                return Phase.CANCELLED;
            case "FAILED":
            case "TIMEOUT":
            case "NODE_FAIL":
            case "PREEMPTED":
            case "BOOT_FAIL":
            case "DEADLINE":
            case "OUT_OF_MEMORY":
                return Phase.FAILED;
            default:
                return Phase.UNKNOWN;
        }
    }

    public boolean isTerminal() {
        return phase().isTerminal();
    }

    /** True while Slurm still runs the epilog or cleans up after a job that has otherwise ended. */
    public boolean isCompleting() {
        return flags().contains("COMPLETING");
    }

    @AutoValue.Builder
    public abstract static class Builder {
        public abstract Builder jobId(long jobId);
        public abstract Builder state(String state);
        public abstract Builder flags(Iterable<String> flags);
        public abstract Builder reason(String reason);
        public abstract Builder held(boolean held);
        public abstract Builder exitCode(Integer exitCode);
        public abstract Builder signal(String signal);
        public abstract Builder nodes(String nodes);
        public abstract Builder startTime(Long startTime);
        public abstract Builder endTime(Long endTime);
        public abstract Builder source(Source source);
        public abstract SlurmJobState build();
    }
}
