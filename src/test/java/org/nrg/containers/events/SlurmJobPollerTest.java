package org.nrg.containers.events;

import com.github.dockerjava.api.model.TaskState;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.nrg.containers.api.slurm.SlurmClient;
import org.nrg.containers.api.slurm.SlurmClientFactory;
import org.nrg.containers.api.slurm.SlurmJobBuilder;
import org.nrg.containers.api.slurm.SlurmJobPaths;
import org.nrg.containers.api.slurm.SlurmJobSnapshot;
import org.nrg.containers.api.slurm.SlurmJobState;
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
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class SlurmJobPollerTest {
    private static final long DATABASE_ID = 42L;
    private static final long JOB_ID = 1234L;
    private static final Instant NOW = Instant.parse("2026-11-06T20:00:00Z");

    @TempDir
    Path buildPath;

    private ContainerService containerService;
    private NrgEventServiceI eventService;
    private SlurmClientFactory clientFactory;
    private SlurmClient client;
    private DockerServer server;
    private SlurmJobPoller poller;
    private MutableClock clock;

    private static final class MutableClock extends Clock {
        private Instant now;

        private MutableClock(final Instant now) {
            this.now = now;
        }

        @Override
        public java.time.ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(final java.time.ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now;
        }
    }

    @BeforeEach
    void setUp() throws Exception {
        containerService = mock(ContainerService.class);
        eventService = mock(NrgEventServiceI.class);
        client = mock(SlurmClient.class);
        clientFactory = mock(SlurmClientFactory.class);
        when(clientFactory.getClient(any())).thenReturn(client);
        final SiteConfigPreferences preferences = mock(SiteConfigPreferences.class);
        when(preferences.getBuildPath()).thenReturn(buildPath.toString());

        server = DockerServer.builder().name("hpc").backend(Backend.SLURM)
                .slurm(SlurmSettings.builder().restUrl("https://slurm:6820").user("xnat").token("t").build())
                .build();

        poller = new SlurmJobPoller(containerService, eventService, clientFactory, preferences);
        clock = new MutableClock(NOW);
        poller.setClock(clock);
        poller.setAdminUser(() -> mock(UserI.class));

        track(container(DATABASE_ID, String.valueOf(JOB_ID)));
        when(client.getJobs(any())).thenReturn(SlurmJobSnapshot.create(1000L, ImmutableMap.of()));
    }

    private static Container container(final long databaseId, final String serviceId) {
        return Container.builder()
                .databaseId(databaseId)
                .commandId(1L)
                .wrapperId(1L)
                .userId("admin")
                .backend(Backend.SLURM)
                .serviceId(serviceId)
                .dockerImage("")
                .commandLine("")
                .build();
    }

    private void track(final Container... containers) {
        when(containerService.retrieveNonfinalizedServiceIds())
                .thenReturn(Arrays.stream(containers).map(Container::databaseId).collect(java.util.stream.Collectors.toList()));
        for (final Container container : containers) {
            when(containerService.retrieveServiceForPoll(container.databaseId())).thenReturn(container);
        }
    }

    private static SlurmJobState.Builder job(final String state) {
        return SlurmJobState.builder().jobId(JOB_ID).state(state).source(SlurmJobState.Source.CONTROLLER)
                .submitTime(NOW.minusSeconds(120).getEpochSecond());
    }

    private void changed(final SlurmJobState... jobs) throws Exception {
        final ImmutableMap.Builder<Long, SlurmJobState> map = ImmutableMap.builder();
        for (final SlurmJobState job : jobs) {
            map.put(job.jobId(), job);
        }
        when(client.getJobs(any())).thenReturn(SlurmJobSnapshot.create(2000L, map.build()));
    }

    private void writeExitCode(final String content) throws Exception {
        final Path file = Path.of(SlurmJobPaths.forContainer(buildPath.toString(), DATABASE_ID, null, null).xnatFile(SlurmJobPaths.EXIT_CODE));
        Files.createDirectories(file.getParent());
        Files.write(file, content.getBytes(StandardCharsets.US_ASCII));
    }

    private ServiceTask onlyTask() {
        final ArgumentCaptor<ServiceTaskEvent> captor = ArgumentCaptor.forClass(ServiceTaskEvent.class);
        verify(eventService).triggerEvent(captor.capture());
        final ServiceTaskEvent event = captor.getValue();
        assertEquals(ServiceTaskEvent.EventType.ProcessTask, event.eventType());
        return event.task();
    }

    private List<SlurmJobPoller.Outcome> poll() throws Exception {
        return poller.poll(server, () -> true);
    }

    @Test
    void runningJobBecomesARunningTask() throws Exception {
        final long start = NOW.minusSeconds(60).getEpochSecond();
        changed(job("RUNNING").nodes("node01").startTime(start).build());

        final List<SlurmJobPoller.Outcome> outcomes = poll();

        final ServiceTask task = onlyTask();
        assertEquals(TaskState.RUNNING.getValue(), task.status());
        assertEquals("1234", task.serviceId());
        assertEquals("1234", task.taskId());
        assertEquals("node01", task.nodeId());
        assertEquals(Instant.ofEpochSecond(start).toString(), task.statusTime());
        assertEquals("Slurm job 1234 running on node01", task.message());
        assertFalse(task.swarmNodeError());
        assertTrue(outcomes.get(0).successful);
        verify(client, never()).getJob(anyLong());
    }

    @Test
    void theIncrementalQueryStartsNearNowAndFollowsSlurmsCursor() throws Exception {
        poll();
        verify(client).getJobs(NOW.minus(SlurmJobPoller.CURSOR_MARGIN).getEpochSecond());
        poll();
        verify(client).getJobs(1000L);
    }

    @Test
    void unknownJobsAreLookedUpOnceThenRemembered() throws Exception {
        when(client.getJob(JOB_ID)).thenReturn(Optional.of(job("PENDING").reason("Priority").build()));

        poll();
        poll();

        verify(client, times(1)).getJob(JOB_ID);
        final ArgumentCaptor<ServiceTaskEvent> captor = ArgumentCaptor.forClass(ServiceTaskEvent.class);
        verify(eventService, times(2)).triggerEvent(captor.capture());
        // Same state, same message and time: processEvent records it once.
        assertEquals(captor.getAllValues().get(0).task(), captor.getAllValues().get(1).task());
        assertEquals("Slurm job 1234 pending (Priority)", captor.getValue().task().message());
    }

    @Test
    void everyJobIsLookedUpAgainAfterTheRefreshInterval() throws Exception {
        when(client.getJob(JOB_ID)).thenReturn(Optional.of(job("RUNNING").build()));
        poll();
        clock.now = NOW.plus(SlurmJobPoller.FULL_REFRESH_INTERVAL).plusSeconds(1);
        poll();
        verify(client, times(2)).getJob(JOB_ID);
    }

    @Test
    void aNewClientStartsFromScratch() throws Exception {
        when(client.getJob(JOB_ID)).thenReturn(Optional.of(job("RUNNING").build()));
        poll();
        final SlurmClient replacement = mock(SlurmClient.class);
        when(replacement.getJobs(any())).thenReturn(SlurmJobSnapshot.create(5L, ImmutableMap.of()));
        when(replacement.getJob(JOB_ID)).thenReturn(Optional.of(job("RUNNING").build()));
        when(clientFactory.getClient(any())).thenReturn(replacement);

        poll();

        verify(replacement).getJobs(NOW.minus(SlurmJobPoller.CURSOR_MARGIN).getEpochSecond());
        verify(replacement).getJob(JOB_ID);
    }

    @Test
    void theExitCodeFileDecidesTheExitCodeOfAFinishedJob() throws Exception {
        writeExitCode("0\n");
        changed(job("COMPLETED").exitCode(0).endTime(NOW.getEpochSecond()).build());

        poll();

        final ServiceTask task = onlyTask();
        assertEquals(TaskState.COMPLETE.getValue(), task.status());
        assertEquals(0L, task.exitCode());
        assertEquals("Slurm job 1234 completed", task.message());
        assertEquals(NOW.toString(), task.statusTime());
    }

    /** The file holds the container's own exit status, so it wins when Slurm reports something else. */
    @Test
    void theExitCodeFileOverridesSlurmsExitCode() throws Exception {
        writeExitCode("3\n");
        changed(job("COMPLETED").exitCode(0).endTime(NOW.getEpochSecond()).build());

        poll();

        final ServiceTask task = onlyTask();
        assertEquals(TaskState.FAILED.getValue(), task.status());
        assertEquals(3L, task.exitCode());
    }

    /** A shared filesystem may not show the file to XNAT yet; Slurm's own exit code is used instead. */
    @Test
    void aFinishedJobWithoutAnExitCodeFileUsesSlurmsExitCode() throws Exception {
        changed(job("FAILED").exitCode(2).reason("NonZeroExitCode").endTime(NOW.getEpochSecond()).build());

        poll();

        final ServiceTask task = onlyTask();
        assertEquals(TaskState.FAILED.getValue(), task.status());
        assertEquals(2L, task.exitCode());
        assertEquals("Slurm job 1234 failed, exit code 2", task.message());
    }

    @Test
    void failuresExplainWhatHappened() throws Exception {
        changed(job("TIMEOUT").exitCode(0).signal("TERM").endTime(NOW.getEpochSecond()).build());
        poll();
        final ServiceTask task = onlyTask();
        assertEquals(TaskState.FAILED.getValue(), task.status());
        assertEquals("Slurm job 1234 reached its time limit, exit code 0, signal TERM", task.message());
    }

    @Test
    void anUnavailableImageIsExplained() throws Exception {
        writeExitCode("125");
        changed(job("FAILED").exitCode(125).reason("NonZeroExitCode").build());
        poll();
        final ServiceTask task = onlyTask();
        assertEquals(125L, task.exitCode());
        assertEquals("Slurm job 1234 failed, exit code 125 (image not available; see stderr)", task.message());
    }

    @Test
    void aPurgedJobIsDecidedByItsExitCodeFile() throws Exception {
        when(client.getJob(JOB_ID)).thenReturn(Optional.empty());
        writeExitCode("3");

        poll();

        final ServiceTask task = onlyTask();
        assertEquals(TaskState.FAILED.getValue(), task.status());
        assertEquals(3L, task.exitCode());
    }

    @Test
    void aPurgedJobWithoutAnExitCodeFails() throws Exception {
        when(client.getJob(JOB_ID)).thenReturn(Optional.empty());
        poll();
        final ServiceTask task = onlyTask();
        assertEquals(TaskState.FAILED.getValue(), task.status());
        assertNull(task.exitCode());
        assertEquals("Slurm no longer knows job 1234 and it left no exit code", task.message());
    }

    @Test
    void aJobLeftHeldIsCancelledAfterTheLimit() throws Exception {
        final long submitted = NOW.minus(SlurmJobPoller.UNRELEASED_HOLD_LIMIT).minusSeconds(1).getEpochSecond();
        changed(job("PENDING").held(true).reason("JobHeldUser").submitTime(submitted).build());

        poll();

        verify(client).cancel(JOB_ID);
        final ServiceTask task = onlyTask();
        assertEquals(TaskState.FAILED.getValue(), task.status());
        assertTrue(task.message().contains("never released"));
    }

    @Test
    void recentlyHeldAndAdminHeldJobsAreLeftAlone() throws Exception {
        changed(job("PENDING").held(true).reason("JobHeldUser").build());
        poll();
        changed(job("PENDING").held(true).reason("JobHeldAdmin").submitTime(NOW.minus(Duration.ofDays(1)).getEpochSecond()).build());
        poll();
        verify(client, never()).cancel(anyLong());
    }

    @Test
    void finalizingTerminalAndNonSlurmContainersAreSkipped() throws Exception {
        final Container swarm = container(43L, "svc").toBuilder().backend(Backend.SWARM).build();
        final Container finalizing = container(44L, "99");
        track(container(DATABASE_ID, String.valueOf(JOB_ID)), swarm, finalizing);
        when(containerService.isFinalizing(finalizing)).thenReturn(true);
        changed(job("RUNNING").build());

        poll();

        verify(containerService, never()).isFinalizing(swarm);
        verify(eventService, times(1)).triggerEvent(any(ServiceTaskEvent.class));
    }

    @Test
    void waitingContainersGetAWaitingEvent() throws Exception {
        final Container full = mock(Container.class);
        final ServiceTask last = ServiceTask.builder().serviceId("1234").status("complete").swarmNodeError(false).build();
        when(full.makeTaskFromLastHistoryItem()).thenReturn(last);
        when(containerService.isWaiting(any())).thenReturn(true);
        when(containerService.get(DATABASE_ID)).thenReturn(full);

        poll();

        final ArgumentCaptor<ServiceTaskEvent> captor = ArgumentCaptor.forClass(ServiceTaskEvent.class);
        verify(eventService).triggerEvent(captor.capture());
        assertEquals(ServiceTaskEvent.EventType.Waiting, captor.getValue().eventType());
        verify(client, never()).getJob(anyLong());
    }

    @Test
    void aBadJobIdIsReportedAndTheRestContinue() throws Exception {
        track(container(45L, "not-a-number"), container(DATABASE_ID, String.valueOf(JOB_ID)));
        changed(job("RUNNING").build());

        final List<SlurmJobPoller.Outcome> outcomes = poll();

        assertFalse(outcomes.get(0).successful);
        assertTrue(outcomes.get(1).successful);
        verify(eventService, times(1)).triggerEvent(any(ServiceTaskEvent.class));
    }

    @Test
    void theWalkStopsWhenLeadershipIsLost() throws Exception {
        changed(job("RUNNING").build());
        poller.poll(server, () -> false);
        verify(containerService, never()).retrieveServiceForPoll(anyLong());
        verifyNoInteractions(eventService);
    }

    @Test
    void nothingToTrackMeansNoSlurmRequests() throws Exception {
        when(containerService.retrieveNonfinalizedServiceIds()).thenReturn(Collections.emptyList());
        poll();
        verifyNoInteractions(clientFactory, client);
    }

    @Test
    void exitCodeFilesAreReadStrictly(@TempDir final Path dir) throws Exception {
        final Path file = dir.resolve("exit-code");
        assertNull(SlurmJobPoller.readExitCode(file));
        Files.write(file, "0\n".getBytes(StandardCharsets.US_ASCII));
        assertEquals(0, SlurmJobPoller.readExitCode(file));
        Files.write(file, String.valueOf(SlurmJobBuilder.EXIT_IMAGE_UNAVAILABLE).getBytes(StandardCharsets.US_ASCII));
        assertEquals(125, SlurmJobPoller.readExitCode(file));
        Files.write(file, "oops".getBytes(StandardCharsets.US_ASCII));
        assertNull(SlurmJobPoller.readExitCode(file));
        Files.write(file, new byte[0]);
        assertNull(SlurmJobPoller.readExitCode(file));
    }
}
