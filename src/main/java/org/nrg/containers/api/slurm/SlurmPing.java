package org.nrg.containers.api.slurm;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;
import java.time.Instant;

/** Result of pinging slurmrestd, which in turn pings each slurmctld. */
@AutoValue
public abstract class SlurmPing {
    /** The slurmrestd API version the client is using. */
    public abstract String apiVersion();

    /** Slurm release reported by slurmrestd, e.g. {@code 23.11.4}. */
    @Nullable public abstract String release();

    @Nullable public abstract String cluster();

    public abstract ImmutableList<String> controllersUp();
    public abstract ImmutableList<String> controllersDown();

    /** True when slurmrestd also serves the slurmdbd endpoints, so purged jobs can still be looked up. */
    public abstract boolean accountingAvailable();

    @Nullable public abstract Instant tokenExpiresAt();

    public static SlurmPing create(final String apiVersion, @Nullable final String release, @Nullable final String cluster,
                                   final Iterable<String> controllersUp, final Iterable<String> controllersDown,
                                   final boolean accountingAvailable, @Nullable final Instant tokenExpiresAt) {
        return new AutoValue_SlurmPing(apiVersion, release, cluster, ImmutableList.copyOf(controllersUp),
                ImmutableList.copyOf(controllersDown), accountingAvailable, tokenExpiresAt);
    }

    /** True when at least one slurmctld answered. */
    public boolean ok() {
        return !controllersUp().isEmpty();
    }
}
