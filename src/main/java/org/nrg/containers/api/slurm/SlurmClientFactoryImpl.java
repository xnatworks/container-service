package org.nrg.containers.api.slurm;

import lombok.extern.slf4j.Slf4j;
import org.nrg.containers.model.server.docker.SlurmSettings;
import org.springframework.stereotype.Service;

/**
 * Caches one client. Settings are compared in full, token and API version included, so any change produces a new
 * client, which detects the API version again on first use.
 */
@Slf4j
@Service
public class SlurmClientFactoryImpl implements SlurmClientFactory {
    private SlurmSettings cachedSettings;
    private SlurmClient cachedClient;

    @Override
    public synchronized SlurmClient getClient(final SlurmSettings settings) {
        if (cachedClient == null || !settings.equals(cachedSettings)) {
            log.debug("Creating a Slurm client for {}", settings.restUrl());
            cachedClient = new SlurmClientImpl(settings);
            cachedSettings = settings;
        }
        return cachedClient;
    }
}
