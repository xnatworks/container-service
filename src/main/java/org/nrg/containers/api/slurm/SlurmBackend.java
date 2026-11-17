package org.nrg.containers.api.slurm;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.nrg.containers.api.LogChunk;
import org.nrg.containers.api.LogType;
import org.nrg.containers.exceptions.ContainerBackendException;
import org.nrg.containers.exceptions.ContainerException;
import org.nrg.containers.exceptions.NoContainerServerException;
import org.nrg.containers.model.container.auto.Container;
import org.nrg.containers.model.server.docker.DockerServerBase.DockerServer;
import org.nrg.containers.model.server.docker.SlurmSettings;
import org.nrg.xdat.preferences.SiteConfigPreferences;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.stream.Stream;

/**
 * Runs containers as Slurm jobs for {@link org.nrg.containers.api.DockerControlApi}.
 * <p>
 * Each container gets a job directory, {@code <build path>/slurm/<container id>} (see
 * {@link SlurmJobPaths#forContainer}), created by XNAT before submission. It holds stdout, stderr, the exit-code
 * file and Apptainer's work directory, so the compute nodes' Slurm account must be able to write to it.
 * <p>
 * Lifecycle, mirroring Swarm's create-then-scale-to-one: {@link #create} submits the job held, {@link #start}
 * releases it, {@link #kill} cancels it, and {@link #remove} (run by auto-cleanup after finalizing has saved the
 * logs) deletes the job directory.
 */
@Slf4j
@Service
public class SlurmBackend {
    /** Largest log returned whole, e.g. to finalizing; a longer one keeps its end. */
    static final int MAX_LOG_BYTES = 64 * 1024 * 1024;

    /** Largest piece of log returned per live poll. */
    static final int MAX_LOG_CHUNK_BYTES = 1024 * 1024;

    private final SlurmClientFactory clientFactory;
    @Nullable private final SiteConfigPreferences siteConfigPreferences;

    @Autowired
    public SlurmBackend(final SlurmClientFactory clientFactory, @Nullable final SiteConfigPreferences siteConfigPreferences) {
        this.clientFactory = clientFactory;
        this.siteConfigPreferences = siteConfigPreferences;
    }

    public String ping(final DockerServer server) throws ContainerBackendException, NoContainerServerException {
        final SlurmPing ping = client(server).ping();
        if (!ping.ok()) {
            throw new ContainerBackendException("slurmrestd answered, but no slurmctld responded: " + ping.controllersDown());
        }
        return "OK";
    }

    /**
     * Creates the job directory and submits the job held.
     *
     * @return the Slurm job ID, stored as the container's service ID
     */
    public String create(final Container toCreate, final DockerServer server)
            throws ContainerException, ContainerBackendException, NoContainerServerException {
        final SlurmSettings settings = settings(server);
        final SlurmJobPaths paths = paths(server, toCreate.databaseId());
        prepareJobDirectory(paths);
        final SlurmJobRequest request = SlurmJobBuilder.build(toCreate, settings, paths);
        final long jobId = client(server).submit(request);
        log.info("Submitted container {} as Slurm job {} in {}", toCreate.databaseId(), jobId, paths.clusterPath());
        return String.valueOf(jobId);
    }

    public void start(final Container container, final DockerServer server) throws ContainerBackendException, NoContainerServerException {
        client(server).release(jobId(container));
    }

    public void kill(final Container container, final DockerServer server) throws ContainerBackendException, NoContainerServerException {
        final long jobId = jobId(container);
        if (!client(server).cancel(jobId)) {
            log.info("Slurm job {} for container {} had already ended", jobId, container.databaseId());
        }
    }

    /** Cancels the job if it still runs, then deletes its job directory. */
    public void remove(final Container container, final DockerServer server) throws ContainerBackendException, NoContainerServerException {
        if (StringUtils.isNotBlank(container.serviceId())) {
            client(server).cancel(jobId(container));
        }
        final SlurmJobPaths paths = paths(server, container.databaseId());
        try {
            deleteRecursively(Paths.get(paths.xnatPath()));
        } catch (IOException e) {
            throw new ContainerBackendException("Could not delete Slurm job directory " + paths.xnatPath(), e);
        }
    }

    /** The whole log, or its last {@value #MAX_LOG_BYTES} bytes; null if the job has written none yet. */
    @Nullable
    public String getLog(final Container container, final LogType logType, final DockerServer server)
            throws ContainerBackendException, NoContainerServerException {
        final Path file = logFile(container, logType, server);
        try (RandomAccessFile in = new RandomAccessFile(file.toFile(), "r")) {
            final long length = in.length();
            final long start = Math.max(0, length - MAX_LOG_BYTES);
            final String content = read(in, start, (int) (length - start));
            return start == 0 ? content : "[XNAT: showing the last " + MAX_LOG_BYTES + " bytes of a " + length + " byte log]\n" + content;
        } catch (NoSuchFileException | java.io.FileNotFoundException e) {
            return null;
        } catch (IOException e) {
            throw new ContainerBackendException("Could not read " + file, e);
        }
    }

    /** What the log gained since {@code offset}, up to {@value #MAX_LOG_CHUNK_BYTES} bytes at a time. */
    public LogChunk getLogFrom(final Container container, final LogType logType, final long offset, final DockerServer server)
            throws ContainerBackendException, NoContainerServerException {
        final Path file = logFile(container, logType, server);
        try (RandomAccessFile in = new RandomAccessFile(file.toFile(), "r")) {
            final long length = in.length();
            // A shorter file than the offset means it was replaced; start over.
            final long start = offset > length ? 0 : Math.max(0, offset);
            final int available = (int) Math.min(MAX_LOG_CHUNK_BYTES, length - start);
            final int count = available == 0 ? 0 : wholeLines(in, start, available);
            return LogChunk.create(read(in, start, count), start + count);
        } catch (NoSuchFileException | java.io.FileNotFoundException e) {
            return LogChunk.create("", 0);
        } catch (IOException e) {
            throw new ContainerBackendException("Could not read " + file, e);
        }
    }

    // ---------------------------------------------------------------------------------------------------------------

    private static String read(final RandomAccessFile in, final long start, final int count) throws IOException {
        final byte[] buffer = new byte[count];
        in.seek(start);
        in.readFully(buffer);
        return new String(buffer, StandardCharsets.UTF_8);
    }

    /**
     * How many of the bytes to hand out now: up to the last complete line, so a line still being written, or a
     * multi-byte character, is not split between reads. A final line without a newline is not shown live; once the
     * container finishes the UI switches to the saved log, which has it.
     */
    private static int wholeLines(final RandomAccessFile in, final long start, final int count) throws IOException {
        final byte[] buffer = new byte[count];
        in.seek(start);
        in.readFully(buffer);
        int end = count;
        while (end > 0 && buffer[end - 1] != '\n') {
            end--;
        }
        // No newline at all: hand out a full chunk anyway (one very long line), otherwise wait for the line to end.
        return end == 0 && count == MAX_LOG_CHUNK_BYTES ? count : end;
    }

    private Path logFile(final Container container, final LogType logType, final DockerServer server) throws NoContainerServerException {
        final SlurmJobPaths paths = paths(server, container.databaseId());
        return Paths.get(paths.xnatFile(logType == LogType.STDERR ? SlurmJobPaths.STDERR : SlurmJobPaths.STDOUT));
    }

    private static void prepareJobDirectory(final SlurmJobPaths paths) throws ContainerException {
        final Path dir = Paths.get(paths.xnatPath());
        try {
            Files.createDirectories(dir);
            for (final String stale : new String[]{SlurmJobPaths.EXIT_CODE, SlurmJobPaths.STDOUT, SlurmJobPaths.STDERR}) {
                Files.deleteIfExists(dir.resolve(stale));
            }
        } catch (IOException e) {
            throw new ContainerException("Could not create the Slurm job directory " + dir, e);
        }
    }

    /**
     * Deletes the directory and everything in it, carrying on past paths it cannot delete. If anything is left, it
     * throws, naming the first path it could not delete: on NFS that is often a {@code .nfs*} file still held open,
     * or output from a job that was still running when it was cancelled.
     */
    static void deleteRecursively(final Path dir) throws IOException {
        if (!Files.exists(dir)) {
            return;
        }
        final IOException[] firstFailure = new IOException[1];
        try (Stream<Path> walk = Files.walk(dir)) {
            walk.sorted(Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.deleteIfExists(path);
                } catch (IOException e) {
                    // A directory that is not empty because a file in it failed is reported by that file.
                    if (firstFailure[0] == null && !(e instanceof DirectoryNotEmptyException)) {
                        firstFailure[0] = e;
                    }
                    log.debug("Could not delete {}: {}", path, e.toString());
                }
            });
        }
        if (Files.exists(dir)) {
            throw new IOException("Could not delete everything in " + dir
                    + (firstFailure[0] == null ? ": files appeared while it was being deleted" : ""), firstFailure[0]);
        }
    }

    private SlurmJobPaths paths(final DockerServer server, final long databaseId) throws NoContainerServerException {
        final String buildPath = siteConfigPreferences == null ? null : siteConfigPreferences.getBuildPath();
        if (StringUtils.isBlank(buildPath)) {
            throw new NoContainerServerException("The site build path is not configured, so there is nowhere to put Slurm job directories");
        }
        return SlurmJobPaths.forContainer(buildPath, databaseId, server.pathTranslationXnatPrefix(), server.pathTranslationDockerPrefix());
    }

    private SlurmClient client(final DockerServer server) throws NoContainerServerException {
        return clientFactory.getClient(settings(server));
    }

    private static SlurmSettings settings(final DockerServer server) throws NoContainerServerException {
        if (server.slurm() == null) {
            throw new NoContainerServerException("The container server has no Slurm settings");
        }
        return server.slurm();
    }

    private static long jobId(final Container container) throws ContainerBackendException {
        try {
            return Long.parseLong(StringUtils.trimToEmpty(container.serviceId()));
        } catch (NumberFormatException e) {
            throw new ContainerBackendException("Container " + container.databaseId() + " has no valid Slurm job ID: " + container.serviceId());
        }
    }
}
