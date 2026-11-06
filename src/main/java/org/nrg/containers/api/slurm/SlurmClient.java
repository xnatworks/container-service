package org.nrg.containers.api.slurm;

import org.nrg.containers.exceptions.ContainerBackendException;

import javax.annotation.Nullable;
import java.util.Optional;

/**
 * Talks to slurmrestd. One client serves one {@link org.nrg.containers.model.server.docker.SlurmSettings}; get it
 * from {@link SlurmClientFactory} so a configuration change replaces it.
 * <p>
 * Every method throws {@link SlurmApiException} when slurmrestd reports an error, and
 * {@link ContainerBackendException} when it cannot be reached.
 */
public interface SlurmClient {
    /** The API version in use: the configured one, or the newest supported one the server offers. */
    String apiVersion() throws ContainerBackendException;

    SlurmPing ping() throws ContainerBackendException;

    /** Submits a batch job and returns its job ID. */
    long submit(SlurmJobRequest request) throws ContainerBackendException;

    /** Releases a job submitted with {@link SlurmJobRequest#hold()}. */
    void release(long jobId) throws ContainerBackendException;

    /**
     * Cancels a job.
     *
     * @return false if Slurm reports the job unknown or already finished. Cancelling a job that was already
     *         cancelled returns true, since slurmrestd reports that only as a warning.
     */
    boolean cancel(long jobId) throws ContainerBackendException;

    /**
     * Looks the job up in slurmctld and, if slurmctld has purged it, in slurmdbd.
     *
     * @return empty only when neither knows the job (or accounting is not available and slurmctld has purged it)
     */
    Optional<SlurmJobState> getJob(long jobId) throws ContainerBackendException;

    /**
     * Gets every job slurmctld knows that changed after {@code changedSince}, in one request.
     *
     * @param changedSince the {@link SlurmJobSnapshot#lastUpdate()} from the previous call, or null for all jobs
     */
    SlurmJobSnapshot getJobs(@Nullable Long changedSince) throws ContainerBackendException;
}
