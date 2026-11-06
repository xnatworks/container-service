package org.nrg.containers.api.slurm;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.nrg.containers.exceptions.ContainerBackendException;
import org.nrg.containers.model.server.docker.SlurmSettings;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;

/** Sends the configured service account's user name and stored token with every request. */
@Slf4j
public class ServiceAccountCredentialProvider implements SlurmCredentialProvider {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String user;
    private final String token;
    private final Instant expiresAt;

    public ServiceAccountCredentialProvider(final SlurmSettings settings) {
        this.user = settings.user();
        this.token = settings.token();
        this.expiresAt = tokenExpiry(token);
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

    /**
     * Reads the {@code exp} claim from a JWT without verifying it. slurmctld verifies the token; XNAT only uses the
     * expiry to warn admins. Tokens from {@code scontrol token} last 30 minutes unless created with a longer
     * {@code lifespan}, which is the most likely way a working configuration stops working.
     *
     * @return the expiry, or null when the token is not a JWT or has no {@code exp} claim
     */
    @Nullable
    static Instant tokenExpiry(@Nullable final String token) {
        if (StringUtils.isBlank(token)) {
            return null;
        }
        final String[] parts = token.split("\\.");
        if (parts.length < 2) {
            return null;
        }
        try {
            final JsonNode claims = MAPPER.readTree(new String(Base64.getUrlDecoder().decode(parts[1]), StandardCharsets.UTF_8));
            final JsonNode exp = claims.get("exp");
            return exp != null && exp.canConvertToLong() ? Instant.ofEpochSecond(exp.asLong()) : null;
        } catch (Exception e) {
            log.debug("Could not read the expiry from the Slurm token: {}", e.getClass().getSimpleName());
            return null;
        }
    }
}
