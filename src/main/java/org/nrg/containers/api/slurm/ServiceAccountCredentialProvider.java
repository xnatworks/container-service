package org.nrg.containers.api.slurm;

import org.apache.commons.lang3.StringUtils;
import org.nrg.containers.exceptions.ContainerBackendException;
import org.nrg.containers.model.server.docker.SlurmSettings;

import javax.annotation.Nullable;
import java.time.Instant;

/** Sends the configured service account's user name and stored token with every request. */
public class ServiceAccountCredentialProvider implements SlurmCredentialProvider {
    private final String user;
    private final String token;
    private final Instant expiresAt;

    public ServiceAccountCredentialProvider(final SlurmSettings settings) {
        this.user = settings.user();
        this.token = settings.token();
        this.expiresAt = SlurmSettings.jwtExpiry(token);
    }

    @Override
    public SlurmCredentials credentials() throws ContainerBackendException {
        if (StringUtils.isAnyBlank(user, token)) {
            throw new ContainerBackendException("The Slurm user and token must both be configured");
        }
        return SlurmCredentials.create(user, token);
    }

    @Nullable
    @Override
    public Instant tokenExpiresAt() {
        return expiresAt;
    }
}
