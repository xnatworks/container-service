package org.nrg.containers.api.slurm;

import org.nrg.containers.exceptions.ContainerBackendException;

import javax.annotation.Nullable;
import java.time.Instant;

/**
 * Supplies the credentials for each slurmrestd request.
 * <p>
 * Version 1 has one implementation, {@link ServiceAccountCredentialProvider}, which submits every job as the
 * configured service account. Two possible future implementations:
 * <ul>
 *     <li>submit as the launching XNAT user, mapped to a cluster account, so jobs run with that user's quota and
 *     permissions;</li>
 *     <li>mint short-lived tokens from the cluster's JWT signing key instead of storing a long-lived token.</li>
 * </ul>
 * Both would need to know who launched the job, so the client asks for credentials per request instead of reading
 * them once at construction.
 */
public interface SlurmCredentialProvider {
    SlurmCredentials credentials() throws ContainerBackendException;

    /** When the current token expires, if the provider can tell; used to warn admins before it lapses. */
    @Nullable
    Instant tokenExpiresAt();
}
