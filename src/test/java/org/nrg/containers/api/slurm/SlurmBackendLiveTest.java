package org.nrg.containers.api.slurm;

import org.apache.commons.lang3.StringUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.nrg.containers.model.container.auto.Container;
import org.nrg.containers.model.server.docker.Backend;
import org.nrg.containers.model.server.docker.DockerServerBase.DockerServer;
import org.nrg.containers.model.server.docker.SlurmSettings;
import org.nrg.xdat.preferences.SiteConfigPreferences;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Runs the launch lifecycle against a real cluster: create (held), start (release), kill (cancel), remove. Skipped
 * unless {@code SLURM_TEST_URL} and {@code SLURM_TEST_TOKEN_FILE} are set. The local build path is translated to
 * /tmp on the cluster, so this checks the lifecycle, not shared-filesystem logs.
 */
public class SlurmBackendLiveTest {
    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    private SlurmBackend backend;
    private SlurmClient client;
    private DockerServer server;
    private Path buildPath;

    @Before
    public void setUp() throws Exception {
        final String url = System.getenv("SLURM_TEST_URL");
        final String tokenFile = System.getenv("SLURM_TEST_TOKEN_FILE");
        assumeTrue("SLURM_TEST_URL and SLURM_TEST_TOKEN_FILE are not set", StringUtils.isNoneBlank(url, tokenFile));
        final String token = new String(Files.readAllBytes(Paths.get(tokenFile.replaceFirst("^~", System.getProperty("user.home")))),
                StandardCharsets.UTF_8).trim().replaceFirst("^SLURM_JWT=", "");
        final SlurmSettings settings = SlurmSettings.builder().restUrl(url).token(token)
                .user(StringUtils.defaultIfBlank(System.getenv("SLURM_TEST_USER"), "xnat"))
                .partition(System.getenv("SLURM_TEST_PARTITION")).timeLimit("5")
                .pullCacheDirectory("/tmp/xnat-live-test-cache").build();

        buildPath = temp.newFolder("build").toPath();
        server = DockerServer.builder().name("live").backend(Backend.SLURM)
                .pathTranslationXnatPrefix(buildPath.toString())
                .pathTranslationDockerPrefix("/tmp/xnat-live-backend-" + System.currentTimeMillis())
                .slurm(settings).build();
        final SiteConfigPreferences preferences = mock(SiteConfigPreferences.class);
        when(preferences.getBuildPath()).thenReturn(buildPath.toString());
        final SlurmClientFactoryImpl factory = new SlurmClientFactoryImpl();
        backend = new SlurmBackend(factory, preferences);
        client = factory.getClient(settings);
    }

    @Test
    public void createStartKillRemove() throws Exception {
        final Container toCreate = Container.builder().databaseId(42L).commandId(1L).wrapperId(1L).userId("live-test")
                .backend(Backend.SLURM).dockerImage("busybox:latest").overrideEntrypoint(true).commandLine("sleep 300").build();

        final String jobId = backend.create(toCreate, server);
        final Container created = toCreate.toBuilder().serviceId(jobId).build();
        try {
            final SlurmJobState held = client.getJob(Long.parseLong(jobId)).get();
            assertThat(held.toString(), held.held(), is(true));

            backend.start(created, server);
            SlurmJobState state = client.getJob(Long.parseLong(jobId)).get();
            for (int i = 0; i < 60 && state.phase() != SlurmJobState.Phase.RUNNING; i++) {
                Thread.sleep(1000);
                state = client.getJob(Long.parseLong(jobId)).get();
            }
            assertThat(state.toString(), state.phase(), is(SlurmJobState.Phase.RUNNING));

            backend.kill(created, server);
            for (int i = 0; i < 30 && !state.isTerminal(); i++) {
                Thread.sleep(1000);
                state = client.getJob(Long.parseLong(jobId)).get();
            }
            assertThat(state.toString(), state.phase(), is(SlurmJobState.Phase.CANCELLED));
        } finally {
            backend.remove(created, server);
        }
        assertThat(Files.exists(buildPath.resolve("slurm/42")), is(false));
    }
}
