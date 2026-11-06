package org.nrg.containers.api.slurm;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableMap;

import javax.annotation.Nullable;

/** Jobs that changed since a given time, from one {@code GET /jobs} call. */
@AutoValue
public abstract class SlurmJobSnapshot {
    /** slurmctld's last update time; pass it to the next {@link SlurmClient#getJobs(Long)} call. */
    @Nullable public abstract Long lastUpdate();

    /** Changed jobs by job ID. Empty when nothing changed. */
    public abstract ImmutableMap<Long, SlurmJobState> jobs();

    public static SlurmJobSnapshot create(@Nullable final Long lastUpdate, final ImmutableMap<Long, SlurmJobState> jobs) {
        return new AutoValue_SlurmJobSnapshot(lastUpdate, jobs);
    }
}
