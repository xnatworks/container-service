package org.nrg.containers.api.slurm;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;
import org.nrg.containers.api.LogChunk;
import org.nrg.containers.api.LogType;
import org.nrg.containers.exceptions.ContainerBackendException;
import org.nrg.containers.exceptions.NoContainerServerException;
import org.nrg.containers.model.container.auto.Container;
import org.nrg.containers.model.server.docker.Backend;
import org.nrg.containers.model.server.docker.DockerServerBase.DockerServer;
import org.nrg.containers.model.server.docker.SlurmSettings;
import org.nrg.xdat.preferences.SiteConfigPreferences;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SlurmBackendTest {
    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    private SlurmClient client;
    private SiteConfigPreferences preferences;
    private SlurmBackend backend;
    private Path buildPath;
    private DockerServer server;

    @Before
    public void setUp() throws Exception {
        buildPath = temp.newFolder("build").toPath();
        client = mock(SlurmClient.class);
        final SlurmClientFactory factory = mock(SlurmClientFactory.class);
        when(factory.getClient(any())).thenReturn(client);
        preferences = mock(SiteConfigPreferences.class);
        when(preferences.getBuildPath()).thenReturn(buildPath.toString());
        backend = new SlurmBackend(factory, preferences);
        server = DockerServer.builder().name("hpc").backend(Backend.SLURM)
                .pathTranslationXnatPrefix(buildPath.toString()).pathTranslationDockerPrefix("/cluster/build")
                .slurm(SlurmSettings.builder().restUrl("https://slurm:6820").user("xnat").token("t").partition("batch").build())
                .build();
    }

    private static Container container(final String serviceId) {
        return Container.builder().databaseId(42L).commandId(1L).wrapperId(1L).userId("admin").backend(Backend.SLURM)
                .serviceId(serviceId).dockerImage("busybox:latest").commandLine("echo hi").build();
    }

    private Path jobDir() {
        return buildPath.resolve("slurm").resolve("42");
    }

    private void writeLog(final String name, final String content) throws Exception {
        Files.createDirectories(jobDir());
        Files.write(jobDir().resolve(name), content.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void createPreparesTheJobDirectoryAndSubmitsWithTranslatedPaths() throws Exception {
        writeLog(SlurmJobPaths.EXIT_CODE, "1");
        writeLog(SlurmJobPaths.STDOUT, "old run");
        when(client.submit(any())).thenReturn(1234L);

        assertThat(backend.create(container(null), server), is("1234"));

        assertThat(Files.isDirectory(jobDir()), is(true));
        assertThat(Files.exists(jobDir().resolve(SlurmJobPaths.EXIT_CODE)), is(false));
        assertThat(Files.exists(jobDir().resolve(SlurmJobPaths.STDOUT)), is(false));
        final ArgumentCaptor<SlurmJobRequest> request = ArgumentCaptor.forClass(SlurmJobRequest.class);
        verify(client).submit(request.capture());
        assertThat(request.getValue().workingDirectory(), is("/cluster/build/slurm/42"));
        assertThat(request.getValue().standardOutput(), is("/cluster/build/slurm/42/stdout.log"));
        assertThat(request.getValue().partition(), is("batch"));
        assertThat(request.getValue().hold(), is(true));
    }

    @Test
    public void startReleasesAndKillCancels() throws Exception {
        backend.start(container("1234"), server);
        verify(client).release(1234L);
        backend.kill(container("1234"), server);
        verify(client).cancel(1234L);
    }

    @Test
    public void removeCancelsAndDeletesTheJobDirectory() throws Exception {
        writeLog(SlurmJobPaths.STDOUT, "done");
        Files.createDirectories(jobDir().resolve("work/tmp"));

        backend.remove(container("1234"), server);

        verify(client).cancel(1234L);
        assertThat(Files.exists(jobDir()), is(false));
        assertThat(Files.isDirectory(buildPath.resolve("slurm")), is(true));
    }

    @Test
    public void aMissingJobIdIsAnError() {
        try {
            backend.start(container("not-a-job"), server);
            fail("Expected an error");
        } catch (ContainerBackendException | NoContainerServerException e) {
            assertThat(e.getMessage(), containsString("not-a-job"));
        }
    }

    @Test
    public void aMissingBuildPathIsAnError() throws Exception {
        when(preferences.getBuildPath()).thenReturn(" ");
        try {
            backend.create(container(null), server);
            fail("Expected an error");
        } catch (NoContainerServerException e) {
            assertThat(e.getMessage(), containsString("build path"));
        }
    }

    @Test
    public void logsAreReadFromTheJobDirectory() throws Exception {
        assertThat(backend.getLog(container("1"), LogType.STDOUT, server), is(nullValue()));
        writeLog(SlurmJobPaths.STDOUT, "out\nlast line without newline");
        writeLog(SlurmJobPaths.STDERR, "err\n");
        assertThat(backend.getLog(container("1"), LogType.STDOUT, server), is("out\nlast line without newline"));
        assertThat(backend.getLog(container("1"), LogType.STDERR, server), is("err\n"));
    }

    @Test
    public void liveReadsContinueFromTheOffsetInWholeLines() throws Exception {
        assertThat(backend.getLogFrom(container("1"), LogType.STDOUT, 0, server).content(), is(""));

        writeLog(SlurmJobPaths.STDOUT, "one\ntwo\nthr");
        final LogChunk first = backend.getLogFrom(container("1"), LogType.STDOUT, 0, server);
        assertThat(first.content(), is("one\ntwo\n"));
        assertThat(first.nextOffset(), is(8L));

        final LogChunk nothingNew = backend.getLogFrom(container("1"), LogType.STDOUT, first.nextOffset(), server);
        assertThat(nothingNew.content(), is(""));
        assertThat(nothingNew.nextOffset(), is(8L));

        writeLog(SlurmJobPaths.STDOUT, "one\ntwo\nthree\nfour\n");
        final LogChunk second = backend.getLogFrom(container("1"), LogType.STDOUT, first.nextOffset(), server);
        assertThat(second.content(), is("three\nfour\n"));
        assertThat(second.nextOffset(), is(19L));
    }

    @Test
    public void aReplacedLogStartsOver() throws Exception {
        writeLog(SlurmJobPaths.STDOUT, "new\n");
        final LogChunk chunk = backend.getLogFrom(container("1"), LogType.STDOUT, 500, server);
        assertThat(chunk.content(), is("new\n"));
        assertThat(chunk.nextOffset(), is(4L));
    }

    @Test
    public void longLiveLogsArriveInChunks() throws Exception {
        final StringBuilder big = new StringBuilder();
        while (big.length() < SlurmBackend.MAX_LOG_CHUNK_BYTES + 100) {
            big.append("0123456789012345678901234567890123456789\n");
        }
        writeLog(SlurmJobPaths.STDOUT, big.toString());

        final LogChunk first = backend.getLogFrom(container("1"), LogType.STDOUT, 0, server);
        assertThat(first.nextOffset() <= SlurmBackend.MAX_LOG_CHUNK_BYTES, is(true));
        assertThat(first.content().endsWith("\n"), is(true));
        final LogChunk rest = backend.getLogFrom(container("1"), LogType.STDOUT, first.nextOffset(), server);
        assertThat(first.content() + rest.content(), is(big.toString()));
    }

    @Test
    public void pingNeedsAResponsiveController() throws Exception {
        when(client.ping()).thenReturn(SlurmPing.create("v0.0.40", null, null, java.util.Collections.emptyList(),
                java.util.Collections.singletonList("ctl1"), true, null));
        try {
            backend.ping(server);
            fail("Expected the ping to fail");
        } catch (ContainerBackendException e) {
            assertThat(e.getMessage(), startsWith("slurmrestd answered, but no slurmctld responded"));
        }
    }
}
