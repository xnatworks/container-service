package org.nrg.containers.events;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.nrg.containers.api.slurm.SlurmClient;
import org.nrg.containers.api.slurm.SlurmClientFactoryImpl;
import org.nrg.containers.api.slurm.SlurmJobBuilder;
import org.nrg.containers.api.slurm.SlurmJobPaths;
import org.nrg.containers.events.model.ServiceTaskEvent;
import org.nrg.containers.model.container.auto.Container;
import org.nrg.containers.model.container.auto.ServiceTask;
import org.nrg.containers.model.server.docker.Backend;
import org.nrg.containers.model.server.docker.DockerServerBase.DockerServer;
import org.nrg.containers.model.server.docker.SlurmSettings;
import org.nrg.containers.services.ContainerService;
import org.nrg.framework.services.NrgEventServiceI;
import org.nrg.xdat.preferences.SiteConfigPreferences;
import org.nrg.xft.security.UserI;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Polls a real job on a real cluster. Skipped unless {@code SLURM_TEST_URL} and {@code SLURM_TEST_TOKEN_FILE} are set
 * (see SlurmClientLiveTest). Needs apptainer and registry access on the compute nodes.
 */
class SlurmJobPollerLiveTest {
    @TempDir
    Path buildPath;

    private SlurmSettings settings;

    @BeforeEach
    void setUp() throws Exception {
        final String url = System.getenv("SLURM_TEST_URL");
        final String tokenFile = System.getenv("SLURM_TEST_TOKEN_FILE");
        assumeTrue(StringUtils.isNoneBlank(url, tokenFile), "SLURM_TEST_URL and SLURM_TEST_TOKEN_FILE are not set");
        final String token = new String(Files.readAllBytes(Paths.get(tokenFile.replaceFirst("^~", System.getProperty("user.home")))),
                StandardCharsets.UTF_8).trim().replaceFirst("^SLURM_JWT=", "");
        settings = SlurmSettings.builder().restUrl(url).token(token)
                .user(StringUtils.defaultIfBlank(System.getenv("SLURM_TEST_USER"), "xnat"))
                .partition(System.getenv("SLURM_TEST_PARTITION"))
                .pullCacheDirectory("/tmp/xnat-live-test-cache").build();
    }

    @Test
    void aFailingJobIsFollowedToItsEnd() throws Exception {
        final SlurmClientFactoryImpl factory = new SlurmClientFactoryImpl();
        final SlurmClient client = factory.getClient(settings);
        final String jobDir = "/tmp/xnat-live-poll-" + System.currentTimeMillis();
        final Container toRun = Container.builder()
                .databaseId(1L).commandId(1L).wrapperId(1L).userId("live-test").backend(Backend.SLURM)
                .dockerImage("busybox:latest").overrideEntrypoint(true).commandLine("sleep 5; exit 3").build();
        final long jobId = client.submit(SlurmJobBuilder.build(toRun, settings, SlurmJobPaths.create(jobDir, jobDir))
                .toBuilder().hold(false).timeLimitMinutes(5).build());

        final Container tracked = toRun.toBuilder().serviceId(String.valueOf(jobId)).build();
        final ContainerService containerService = mock(ContainerService.class);
        when(containerService.retrieveNonfinalizedServiceIds()).thenReturn(Collections.singletonList(1L));
        when(containerService.retrieveServiceForPoll(1L)).thenReturn(tracked);
        final NrgEventServiceI eventService = mock(NrgEventServiceI.class);
        final SiteConfigPreferences preferences = mock(SiteConfigPreferences.class);
        when(preferences.getBuildPath()).thenReturn(buildPath.toString());

        final SlurmJobPoller poller = new SlurmJobPoller(containerService, eventService, factory, preferences);
        poller.setAdminUser(() -> mock(UserI.class));
        final DockerServer server = DockerServer.builder().name("live").backend(Backend.SLURM).slurm(settings).build();

        final ArgumentCaptor<ServiceTaskEvent> captor = ArgumentCaptor.forClass(ServiceTaskEvent.class);
        ServiceTask last = null;
        for (int i = 0; i < 90 && (last == null || !last.isExitStatus()); i++) {
            poller.poll(server, () -> true);
            verify(eventService, atLeastOnce()).triggerEvent(captor.capture());
            last = captor.getValue().task();
            Thread.sleep(2000);
        }

        final List<String> statuses = captor.getAllValues().stream().map(e -> e.task().status()).distinct().collect(Collectors.toList());
        assertTrue(statuses.contains("running"), "saw " + statuses);
        assertEquals("failed", last.status(), last.toString());
        assertEquals(3L, last.exitCode(), last.toString());
        assertEquals("Slurm job " + jobId + " failed, exit code 3", last.message());
    }
}
