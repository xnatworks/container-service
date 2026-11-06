package org.nrg.containers.api.slurm;

import org.apache.commons.lang3.StringUtils;
import org.junit.Before;
import org.junit.Test;
import org.nrg.containers.model.server.docker.SlurmSettings;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Optional;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeTrue;

/**
 * Runs against a real slurmrestd. Skipped unless {@code SLURM_TEST_URL} and {@code SLURM_TEST_TOKEN_FILE} are set:
 * <pre>
 * SLURM_TEST_URL=http://slurm.example.org:6820 SLURM_TEST_TOKEN_FILE=~/.slurm-test-token \
 *     ./gradlew test --tests '*SlurmClientLiveTest'
 * </pre>
 * Optional: {@code SLURM_TEST_USER} (default xnat), {@code SLURM_TEST_PARTITION}. Submits one held {@code sleep}
 * job, releases it and cancels it.
 */
public class SlurmClientLiveTest {
    private SlurmClient client;
    private SlurmSettings settings;

    @Before
    public void setUp() throws Exception {
        final String url = System.getenv("SLURM_TEST_URL");
        final String tokenFile = System.getenv("SLURM_TEST_TOKEN_FILE");
        assumeTrue("SLURM_TEST_URL and SLURM_TEST_TOKEN_FILE are not set", StringUtils.isNoneBlank(url, tokenFile));
        final String token = new String(Files.readAllBytes(Paths.get(tokenFile.replaceFirst("^~", System.getProperty("user.home")))),
                StandardCharsets.UTF_8).trim().replaceFirst("^SLURM_JWT=", "");
        settings = SlurmSettings.builder().restUrl(url).token(token)
                .user(StringUtils.defaultIfBlank(System.getenv("SLURM_TEST_USER"), "xnat"))
                .partition(System.getenv("SLURM_TEST_PARTITION")).build();
        client = new SlurmClientImpl(settings);
    }

    @Test
    public void submitReleaseAndCancel() throws Exception {
        final SlurmPing ping = client.ping();
        assertThat(ping.toString(), ping.ok(), is(true));

        final long jobId = client.submit(SlurmJobRequest.builder().name("xnat-live-test")
                .script("#!/bin/bash\nsleep 120\n").workingDirectory("/tmp")
                .standardOutput("/tmp/xnat-live-test-%j.out").timeLimitMinutes(5).build());
        try {
            final SlurmJobState held = client.getJob(jobId).get();
            assertThat(held.toString(), held.phase(), is(SlurmJobState.Phase.PENDING));
            assertThat(held.toString(), held.held(), is(true));

            client.release(jobId);
            final SlurmJobState released = waitWhilePending(jobId);
            assertThat(released.toString(), released.held(), is(false));

            final Long cursor = client.getJobs(null).lastUpdate();
            assertThat(client.getJobs(cursor).lastUpdate(), is(cursor));
        } finally {
            client.cancel(jobId);
        }
        final SlurmJobState cancelled = waitWhileActive(jobId);
        assertThat(cancelled.toString(), cancelled.phase(), is(SlurmJobState.Phase.CANCELLED));
        assertThat(client.getJob(67_000_000L), is(Optional.empty()));
        assertThat(client.cancel(67_000_000L), is(false));
    }

    /**
     * Runs a built job script on the cluster without needing apptainer there: with registry pulls disabled and no
     * SIF, the script must fail the job with exit code 125 before it reaches the runtime.
     */
    @Test
    public void builtScriptRunsUnderSlurm() throws Exception {
        final org.nrg.containers.model.container.auto.Container container = org.nrg.containers.model.container.auto.Container.builder()
                .databaseId(1L).commandId(1L).wrapperId(1L).userId("live-test")
                .backend(org.nrg.containers.model.server.docker.Backend.SLURM)
                .dockerImage("busybox:latest").commandLine("echo hello").build();
        final String jobDir = "/tmp/xnat-live-test-" + System.currentTimeMillis();
        final SlurmJobRequest request = SlurmJobBuilder.build(container,
                settings.toBuilder().allowDockerPull(false).sifDirectory("/nonexistent/sif").build(),
                SlurmJobPaths.create(jobDir, jobDir));

        final long jobId = client.submit(request.toBuilder().hold(false).timeLimitMinutes(2).build());
        final SlurmJobState state = waitWhileActive(jobId);

        assertThat(state.toString(), state.phase(), is(SlurmJobState.Phase.FAILED));
        assertThat(state.toString(), state.exitCode(), is(SlurmJobBuilder.EXIT_IMAGE_UNAVAILABLE));
    }

    /**
     * Runs busybox with apptainer on the cluster (needs registry access from the node). Each container exits 7 only
     * if every check inside it passes, because the job's output stays on the cluster.
     */
    @Test
    public void containersRunWithApptainer() throws Exception {
        final String stamp = String.valueOf(System.currentTimeMillis());
        final SlurmSettings runSettings = settings.toBuilder().pullCacheDirectory("/tmp/xnat-live-test-cache").build();

        // exec mode: environment, --pwd, writable /tmp, read-only bind
        final String checks = "test \"$GREETING\" = 'hello world'"
                + " && test \"$(pwd)\" = /tmp"
                + " && touch /tmp/written"
                + " && test -f /hostetc/hostname"
                + " && ! touch /hostetc/xnat-should-fail 2>/dev/null"
                + " && exit 7; exit 1";
        final org.nrg.containers.model.container.auto.Container exec = liveContainer()
                .overrideEntrypoint(true)
                .commandLine(checks)
                .workingDirectory("/tmp")
                .environmentVariables(com.google.common.collect.ImmutableMap.of("GREETING", "hello world"))
                .mounts(java.util.Collections.singletonList(org.nrg.containers.model.container.auto.Container.ContainerMount.builder()
                        .databaseId(0L).name("etc").writable(false).xnatHostPath("/etc").containerHostPath("/etc")
                        .containerPath("/hostetc").inputFiles(java.util.Collections.emptyList()).build()))
                .build();
        assertThat(runToEnd(exec, runSettings, "/tmp/xnat-live-exec-" + stamp).exitCode(), is(7));

        // run mode: arguments reach the image's default command exactly, including $, quotes, backslashes and
        // backticks that apptainer's runscript would otherwise evaluate
        final org.nrg.containers.model.container.auto.Container run = liveContainer()
                .commandLine("sh -c 'test \"$0|$1|$2\" = \"two words|\\$HOME \\\"q\\\"|a\\\\b\\`x\\`\" && exit 7; exit 1'"
                        + " \"two words\" '$HOME \"q\"' 'a\\b`x`'")
                .build();
        assertThat(runToEnd(run, runSettings, "/tmp/xnat-live-run-" + stamp).exitCode(), is(7));
    }

    private static org.nrg.containers.model.container.auto.Container.Builder liveContainer() {
        return org.nrg.containers.model.container.auto.Container.builder()
                .databaseId(1L).commandId(1L).wrapperId(1L).userId("live-test")
                .backend(org.nrg.containers.model.server.docker.Backend.SLURM)
                .dockerImage("busybox:latest");
    }

    private SlurmJobState runToEnd(final org.nrg.containers.model.container.auto.Container container,
                                   final SlurmSettings runSettings, final String jobDir) throws Exception {
        final SlurmJobRequest request = SlurmJobBuilder.build(container, runSettings, SlurmJobPaths.create(jobDir, jobDir));
        final long jobId = client.submit(request.toBuilder().hold(false).timeLimitMinutes(5).build());
        SlurmJobState state = client.getJob(jobId).get();
        for (int i = 0; i < 180 && !state.isTerminal(); i++) {
            Thread.sleep(1000);
            state = client.getJob(jobId).get();
        }
        assertThat(state.toString(), state.isTerminal(), is(true));
        return state;
    }

    private SlurmJobState waitWhilePending(final long jobId) throws Exception {
        SlurmJobState state = client.getJob(jobId).get();
        for (int i = 0; i < 30 && state.phase() == SlurmJobState.Phase.PENDING && !state.held(); i++) {
            Thread.sleep(1000);
            state = client.getJob(jobId).get();
        }
        assertThat(state.toString(), state.phase(), not(SlurmJobState.Phase.UNKNOWN));
        return state;
    }

    private SlurmJobState waitWhileActive(final long jobId) throws Exception {
        SlurmJobState state = client.getJob(jobId).get();
        for (int i = 0; i < 30 && !state.isTerminal(); i++) {
            Thread.sleep(1000);
            state = client.getJob(jobId).get();
        }
        return state;
    }
}
