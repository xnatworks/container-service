package org.nrg.containers.events;

import com.github.dockerjava.api.model.TaskState;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.nrg.containers.api.slurm.SlurmClient;
import org.nrg.containers.api.slurm.SlurmClientFactory;
import org.nrg.containers.api.slurm.SlurmJobBuilder;
import org.nrg.containers.api.slurm.SlurmJobPaths;
import org.nrg.containers.api.slurm.SlurmJobSnapshot;
import org.nrg.containers.api.slurm.SlurmJobState;
import org.nrg.containers.events.model.ServiceTaskEvent;
import org.nrg.containers.exceptions.ContainerBackendException;
import org.nrg.containers.model.container.auto.Container;
import org.nrg.containers.model.container.auto.ServiceTask;
import org.nrg.containers.model.server.docker.Backend;
import org.nrg.containers.model.server.docker.DockerServerBase.DockerServer;
import org.nrg.containers.services.ContainerService;
import org.nrg.framework.services.NrgEventServiceI;
import org.nrg.xdat.preferences.SiteConfigPreferences;
import org.nrg.xdat.security.helpers.Users;
import org.nrg.xft.security.UserI;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

/**
 * Polls Slurm for the jobs behind unfinalized containers and turns their states into {@link ServiceTaskEvent}s, the
 * same events Swarm produces, so history, workflow updates and finalizing work unchanged. Called by
 * {@link ContainerStatusUpdater} on the primary node only.
 * <p>
 * Requests stay proportional to XNAT's jobs, never to the cluster's: each tick asks slurmctld once for jobs changed
 * since its last answer, and looks up individually only jobs it has no state for. Every
 * {@link #FULL_REFRESH_INTERVAL} it forgets what it knows and looks every job up again, which covers any change the
 * incremental query missed.
 * <p>
 * Once a job ends, its exit code comes from the file the batch script writes in the job directory, then from Slurm.
 * When slurmctld has purged a job, slurmdbd is asked, and failing that the exit-code file alone decides.
 */
@Slf4j
@Component
public class SlurmJobPoller {
    static final Duration FULL_REFRESH_INTERVAL = Duration.ofMinutes(10);

    /**
     * A job is submitted held and released by the launch right after. One still held by its submitter this long
     * after submission was never released, because the launch failed partway, so it is cancelled.
     */
    static final Duration UNRELEASED_HOLD_LIMIT = Duration.ofMinutes(15);

    /** Allows for the XNAT host's clock running ahead of slurmctld's when seeding the incremental query. */
    static final Duration CURSOR_MARGIN = Duration.ofMinutes(1);

    private static final int MAX_EXIT_CODE_FILE_BYTES = 32;
    private static final String HELD_BY_USER = "JobHeldUser";

    private final ContainerService containerService;
    private final NrgEventServiceI eventService;
    private final SlurmClientFactory slurmClientFactory;
    private final SiteConfigPreferences siteConfigPreferences;

    private Supplier<UserI> adminUser = Users::getAdminUser;
    private Clock clock = Clock.systemUTC();

    // Guarded by this.
    private SlurmClient trackedClient;
    private Long cursor;
    private Instant lastFullRefresh;
    private final Map<Long, SlurmJobState> known = new HashMap<>();

    @Autowired
    public SlurmJobPoller(final ContainerService containerService, final NrgEventServiceI eventService,
                          final SlurmClientFactory slurmClientFactory, final SiteConfigPreferences siteConfigPreferences) {
        this.containerService = containerService;
        this.eventService = eventService;
        this.slurmClientFactory = slurmClientFactory;
        this.siteConfigPreferences = siteConfigPreferences;
    }

    /** Result of checking one container. */
    public static final class Outcome {
        public final String id;
        public final boolean successful;
        public final String message;

        private Outcome(final String id, final boolean successful, final String message) {
            this.id = id;
            this.successful = successful;
            this.message = message;
        }

        static Outcome success(final String id) {
            return new Outcome(id, true, null);
        }

        static Outcome failure(final String id, final String message) {
            return new Outcome(id, false, message);
        }
    }

    /** Forgets everything, so the next poll looks every job up again. */
    public synchronized void reset() {
        trackedClient = null;
        cursor = null;
        lastFullRefresh = null;
        known.clear();
    }

    /**
     * Checks every unfinalized Slurm container once.
     *
     * @param stillPrimary checked before each container, so a node that is no longer primary stops sending events
     * @throws ContainerBackendException when slurmrestd cannot be queried at all
     */
    public synchronized List<Outcome> poll(final DockerServer server, final BooleanSupplier stillPrimary) throws ContainerBackendException {
        final List<Long> databaseIds = containerService.retrieveNonfinalizedServiceIds();
        if (databaseIds.isEmpty()) {
            known.clear();
            return Collections.emptyList();
        }

        final SlurmClient client = slurmClientFactory.getClient(server.slurm());
        if (client != trackedClient) {
            reset();
            trackedClient = client;
        }
        final Instant now = clock.instant();
        if (lastFullRefresh == null || now.isAfter(lastFullRefresh.plus(FULL_REFRESH_INTERVAL))) {
            known.clear();
            lastFullRefresh = now;
        }
        if (cursor == null) {
            cursor = now.minus(CURSOR_MARGIN).getEpochSecond();
        }
        final SlurmJobSnapshot changes = client.getJobs(cursor);
        cursor = changes.lastUpdate();

        final List<Outcome> outcomes = new ArrayList<>();
        final Set<Long> tracked = new HashSet<>();
        for (final Long databaseId : databaseIds) {
            if (!stillPrimary.getAsBoolean()) {
                log.debug("No longer the primary node; stopping the Slurm job walk.");
                break;
            }
            try {
                final Container service = containerService.retrieveServiceForPoll(databaseId);
                if (service == null || service.backend() != Backend.SLURM) {
                    continue;
                }
                final Long jobId = parseJobId(service.serviceId());
                if (jobId == null) {
                    outcomes.add(Outcome.failure(service.serviceId(), "Container " + databaseId + " has no valid Slurm job ID"));
                    continue;
                }
                tracked.add(jobId);
                check(client, server, service, jobId, changes);
                outcomes.add(Outcome.success(service.serviceId()));
            } catch (Exception e) {
                log.error("Could not update the status of Slurm container {}.", databaseId, e);
                outcomes.add(Outcome.failure(String.valueOf(databaseId), e.getMessage()));
            }
        }
        known.keySet().retainAll(tracked);
        return outcomes;
    }

    private void check(final SlurmClient client, final DockerServer server, final Container service, final long jobId,
                       final SlurmJobSnapshot changes) throws Exception {
        if (containerService.fixWorkflowContainerStatusMismatch(service, adminUser.get())) {
            log.debug("Slurm container {} job {} had a workflow and status mismatch", service.databaseId(), jobId);
            return;
        }
        if (containerService.isFinalizing(service) || containerService.containerStatusIsTerminal(service)) {
            log.debug("Slurm container {} job {} is no longer unfinalized", service.databaseId(), jobId);
            return;
        }
        if (containerService.isWaiting(service)) {
            final Container full = containerService.get(service.databaseId());
            eventService.triggerEvent(ServiceTaskEvent.create(full.makeTaskFromLastHistoryItem(), full, ServiceTaskEvent.EventType.Waiting));
            return;
        }

        Optional<SlurmJobState> state = Optional.ofNullable(changes.jobs().get(jobId));
        if (!state.isPresent()) {
            state = Optional.ofNullable(known.get(jobId));
        }
        if (!state.isPresent()) {
            state = client.getJob(jobId);
        }
        state.ifPresent(s -> known.put(jobId, s));

        final ServiceTask task;
        if (state.isPresent()) {
            final SlurmJobState job = state.get();
            if (isUnreleased(job)) {
                log.warn("Slurm job {} for container {} was never released; cancelling it.", jobId, service.databaseId());
                client.cancel(jobId);
                known.remove(jobId);
                task = taskBuilder(service, jobId)
                        .status(TaskState.FAILED.getValue())
                        .message("Slurm job " + jobId + " was submitted but never released, so it was cancelled")
                        .build();
            } else {
                task = toTask(service, jobId, job, job.isTerminal() ? readExitCode(server, service) : null);
            }
        } else {
            task = purgedTask(service, jobId, readExitCode(server, service));
        }
        if (task == null) {
            return;
        }
        log.debug("Slurm container {} job {}: {} {}", service.databaseId(), jobId, task.status(), task.message());
        eventService.triggerEvent(ServiceTaskEvent.create(task, service));
    }

    private boolean isUnreleased(final SlurmJobState job) {
        return job.phase() == SlurmJobState.Phase.PENDING && job.held() && HELD_BY_USER.equals(job.reason())
                && job.submitTime() != null
                && clock.instant().isAfter(Instant.ofEpochSecond(job.submitTime()).plus(UNRELEASED_HOLD_LIMIT));
    }

    /**
     * Maps a Slurm job state onto the Swarm task states processEvent understands. Messages and timestamps are stable
     * for a given state, so seeing the same state again on later ticks adds no history.
     *
     * @return null when the state means nothing to report
     */
    @Nullable
    static ServiceTask toTask(final Container service, final long jobId, final SlurmJobState job, @Nullable final Integer exitCodeFromFile) {
        final ServiceTask.Builder task = taskBuilder(service, jobId).nodeId(job.nodes());
        switch (job.phase()) {
            case PENDING:
                return task.status(TaskState.PENDING.getValue())
                        .message("Slurm job " + jobId + " pending" + (job.reason() == null ? "" : " (" + job.reason() + ")"))
                        .statusTime(isoTime(job.submitTime()))
                        .build();
            case RUNNING:
                return task.status(TaskState.RUNNING.getValue())
                        .message("Slurm job " + jobId + " " + job.state().toLowerCase() + (job.nodes() == null ? "" : " on " + job.nodes()))
                        .statusTime(isoTime(job.startTime()))
                        .build();
            case SUCCEEDED:
            case FAILED:
            case CANCELLED:
                final Integer exitCode = exitCodeFromFile != null ? exitCodeFromFile : job.exitCode();
                final boolean succeeded = job.phase() == SlurmJobState.Phase.SUCCEEDED && (exitCode == null || exitCode == 0);
                return task.status(succeeded ? TaskState.COMPLETE.getValue() : TaskState.FAILED.getValue())
                        .exitCode(exitCode == null ? null : exitCode.longValue())
                        .message(endMessage(jobId, job, exitCode))
                        .statusTime(isoTime(job.endTime()))
                        .build();
            default:
                log.debug("Slurm job {} for container {} is in state {}; waiting for a known state", jobId, service.databaseId(), job.state());
                return null;
        }
    }

    /** Neither slurmctld nor slurmdbd knows the job: only the exit-code file can say how it ended. */
    static ServiceTask purgedTask(final Container service, final long jobId, @Nullable final Integer exitCodeFromFile) {
        final ServiceTask.Builder task = taskBuilder(service, jobId);
        if (exitCodeFromFile == null) {
            return task.status(TaskState.FAILED.getValue())
                    .message("Slurm no longer knows job " + jobId + " and it left no exit code")
                    .build();
        }
        return task.status(exitCodeFromFile == 0 ? TaskState.COMPLETE.getValue() : TaskState.FAILED.getValue())
                .exitCode(exitCodeFromFile.longValue())
                .message("Slurm job " + jobId + " exited with " + exitCodeFromFile + describeExitCode(exitCodeFromFile))
                .build();
    }

    private static String endMessage(final long jobId, final SlurmJobState job, @Nullable final Integer exitCode) {
        final StringBuilder message = new StringBuilder("Slurm job ").append(jobId).append(' ');
        switch (job.state()) {
            case "COMPLETED":
                message.append("completed");
                break;
            case "CANCELLED":
                message.append("was cancelled");
                break;
            case "TIMEOUT":
                message.append("reached its time limit");
                break;
            case "OUT_OF_MEMORY":
                message.append("ran out of memory");
                break;
            case "NODE_FAIL":
                message.append("failed because its node failed");
                break;
            case "PREEMPTED":
                message.append("was preempted");
                break;
            default:
                message.append(job.state().toLowerCase());
        }
        if (exitCode != null && !"COMPLETED".equals(job.state())) {
            message.append(", exit code ").append(exitCode).append(describeExitCode(exitCode));
        }
        if (job.signal() != null) {
            message.append(", signal ").append(job.signal());
        }
        return message.toString();
    }

    private static String describeExitCode(final int exitCode) {
        return exitCode == SlurmJobBuilder.EXIT_IMAGE_UNAVAILABLE ? " (image not available; see stderr)" : "";
    }

    private static ServiceTask.Builder taskBuilder(final Container service, final long jobId) {
        return ServiceTask.builder()
                .serviceId(service.serviceId())
                .taskId(String.valueOf(jobId))
                .swarmNodeError(false);
    }

    @Nullable
    private static String isoTime(@Nullable final Long epochSeconds) {
        return epochSeconds == null ? null : Instant.ofEpochSecond(epochSeconds).toString();
    }

    @Nullable
    private static Long parseJobId(@Nullable final String serviceId) {
        try {
            return serviceId == null ? null : Long.valueOf(serviceId.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /** Reads the exit code the batch script wrote, if it is there and well formed. */
    @Nullable
    Integer readExitCode(final DockerServer server, final Container service) {
        final String buildPath = siteConfigPreferences.getBuildPath();
        if (StringUtils.isBlank(buildPath)) {
            return null;
        }
        final SlurmJobPaths paths = SlurmJobPaths.forContainer(buildPath, service.databaseId(),
                server.pathTranslationXnatPrefix(), server.pathTranslationDockerPrefix());
        return readExitCode(Paths.get(paths.xnatFile(SlurmJobPaths.EXIT_CODE)));
    }

    @Nullable
    static Integer readExitCode(final Path file) {
        try (InputStream in = Files.newInputStream(file)) {
            final byte[] buffer = new byte[MAX_EXIT_CODE_FILE_BYTES];
            final int read = in.read(buffer);
            final String text = read <= 0 ? "" : new String(buffer, 0, read, StandardCharsets.US_ASCII).trim();
            return text.matches("\\d{1,3}") ? Integer.valueOf(text) : null;
        } catch (NoSuchFileException e) {
            return null;
        } catch (IOException e) {
            log.debug("Could not read Slurm exit-code file {}: {}", file, e.toString());
            return null;
        }
    }

    // For tests.
    void setClock(final Clock clock) {
        this.clock = clock;
    }

    void setAdminUser(final Supplier<UserI> adminUser) {
        this.adminUser = adminUser;
    }
}
