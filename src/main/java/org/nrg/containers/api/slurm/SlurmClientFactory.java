package org.nrg.containers.api.slurm;

import org.nrg.containers.model.server.docker.SlurmSettings;

/**
 * Provides the {@link SlurmClient} for the current Slurm settings. A client is reused while the settings are equal
 * and replaced as soon as any of them changes, so saving the server configuration needs no explicit reset.
 */
public interface SlurmClientFactory {
    SlurmClient getClient(SlurmSettings settings);
}
