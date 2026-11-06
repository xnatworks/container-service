package org.nrg.containers.model.server.docker;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Connection and job settings for the {@link Backend#SLURM} backend. Jobs are submitted through slurmrestd and run
 * the command's image with Apptainer (or Singularity) on the compute nodes.
 * <p>
 * Authentication uses a single service account: every job is submitted as {@link #user()} with {@link #token()}.
 * Submitting as the launching XNAT user is a possible future mode; keep anything that depends on who submits
 * the job behind the client's credential handling rather than reading these two fields directly.
 * <p>
 * Partition, account, QOS and time limit apply to every job. Per-command overrides are planned future work.
 * <p>
 * The token is write-only over JSON: the server configuration API never returns it, and saving a configuration
 * without one keeps the stored token. {@link #tokenSet()} tells clients whether one is stored.
 */
@AutoValue
public abstract class SlurmSettings implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * Oldest slurmrestd API version XNAT supports. v0.0.40 (Slurm 23.11) introduced the job state arrays and
     * {@code {set, infinite, number}} number wrappers the client relies on; v0.0.38 and v0.0.39 predate them.
     */
    public static final String MIN_API_VERSION = "v0.0.40";

    /**
     * API versions the client knows, oldest first. A blank {@link #apiVersion()} picks the newest of these that the
     * server offers. Each Slurm release serves only a few versions (23.11 has v0.0.38-v0.0.40, 26.05 has
     * v0.0.42-v0.0.45), so no single version covers the clusters XNAT runs against.
     */
    public static final List<String> SUPPORTED_API_VERSIONS = Collections.unmodifiableList(
            Arrays.asList("v0.0.40", "v0.0.41", "v0.0.42", "v0.0.43", "v0.0.44", "v0.0.45"));
    public static final String RUNTIME_APPTAINER = "apptainer";
    public static final String RUNTIME_SINGULARITY = "singularity";
    public static final List<String> RUNTIMES = Collections.unmodifiableList(Arrays.asList(RUNTIME_APPTAINER, RUNTIME_SINGULARITY));

    private static final Pattern API_VERSION_PATTERN = Pattern.compile("^v(\\d+)\\.(\\d+)\\.(\\d+)$");

    /** Base URL of slurmrestd, e.g. {@code https://slurm.example.org:6820}. */
    @Nullable @JsonProperty("rest-url") public abstract String restUrl();

    /**
     * slurmrestd API version to use, e.g. {@code v0.0.40}. Blank means detect it: use the newest of
     * {@link #SUPPORTED_API_VERSIONS} that the server offers.
     */
    @Nullable @JsonProperty("api-version") public abstract String apiVersion();

    /** Cluster account that submits every job (sent as {@code X-SLURM-USER-NAME}). */
    @Nullable @JsonProperty("user") public abstract String user();

    /** JWT for {@link #user()} (sent as {@code X-SLURM-USER-TOKEN}). Never serialized. */
    @Nullable @JsonProperty(value = "token", access = JsonProperty.Access.WRITE_ONLY) public abstract String token();

    @Nullable @JsonProperty("partition") public abstract String partition();
    @Nullable @JsonProperty("account") public abstract String account();
    @Nullable @JsonProperty("qos") public abstract String qos();

    /** Job time limit in any format sbatch accepts, e.g. {@code 90}, {@code 4:00:00} or {@code 1-12:00:00}. */
    @Nullable @JsonProperty("time-limit") public abstract String timeLimit();

    /** Container runtime binary on the compute nodes: {@code apptainer} or {@code singularity}. */
    @JsonProperty("runtime") public abstract String runtime();

    /**
     * Shared directory of pre-built SIF images. When it holds a SIF for a job's image, the job runs it rather than
     * pulling the image.
     */
    @Nullable @JsonProperty("sif-directory") public abstract String sifDirectory();

    /**
     * Whether a job may pull its image from a registry ({@code docker://}) when no SIF is found. Turn this off on
     * clusters whose compute nodes can't reach a registry, so launches fail in XNAT with a clear message instead of
     * failing on the node.
     */
    @JsonProperty("allow-docker-pull") public abstract boolean allowDockerPull();

    /** Shared Apptainer cache directory for pulled images, so each image is converted once rather than per job. */
    @Nullable @JsonProperty("pull-cache-directory") public abstract String pullCacheDirectory();

    @JsonProperty("token-set")
    public boolean tokenSet() {
        return StringUtils.isNotBlank(token());
    }

    @JsonCreator
    public static SlurmSettings create(@JsonProperty("rest-url") final String restUrl,
                                       @JsonProperty("api-version") final String apiVersion,
                                       @JsonProperty("user") final String user,
                                       @JsonProperty("token") final String token,
                                       @JsonProperty("partition") final String partition,
                                       @JsonProperty("account") final String account,
                                       @JsonProperty("qos") final String qos,
                                       @JsonProperty("time-limit") final String timeLimit,
                                       @JsonProperty("runtime") final String runtime,
                                       @JsonProperty("sif-directory") final String sifDirectory,
                                       @JsonProperty("allow-docker-pull") final Boolean allowDockerPull,
                                       @JsonProperty("pull-cache-directory") final String pullCacheDirectory) {
        return builder()
                .restUrl(StringUtils.trimToNull(restUrl))
                .apiVersion(StringUtils.trimToNull(apiVersion))
                .user(StringUtils.trimToNull(user))
                .token(StringUtils.trimToNull(token))
                .partition(StringUtils.trimToNull(partition))
                .account(StringUtils.trimToNull(account))
                .qos(StringUtils.trimToNull(qos))
                .timeLimit(StringUtils.trimToNull(timeLimit))
                .runtime(StringUtils.defaultIfBlank(StringUtils.trim(runtime), RUNTIME_APPTAINER).toLowerCase(Locale.ROOT))
                .sifDirectory(StringUtils.trimToNull(sifDirectory))
                .allowDockerPull(allowDockerPull == null || allowDockerPull)
                .pullCacheDirectory(StringUtils.trimToNull(pullCacheDirectory))
                .build();
    }

    public static SlurmSettings create(final DockerServerEntity entity) {
        return create(entity.getSlurmRestUrl(), entity.getSlurmApiVersion(), entity.getSlurmUser(),
                entity.getSlurmToken(), entity.getSlurmPartition(), entity.getSlurmAccount(), entity.getSlurmQos(),
                entity.getSlurmTimeLimit(), entity.getSlurmRuntime(), entity.getSlurmSifDirectory(),
                entity.getSlurmAllowDockerPull(), entity.getSlurmPullCacheDirectory());
    }

    /** True when the entity has no Slurm settings stored, i.e. it was never configured for Slurm. */
    static boolean isEmpty(final DockerServerEntity entity) {
        return StringUtils.isAllBlank(entity.getSlurmRestUrl(), entity.getSlurmUser(), entity.getSlurmToken());
    }

    public static Builder builder() {
        return new AutoValue_SlurmSettings.Builder()
                .runtime(RUNTIME_APPTAINER)
                .allowDockerPull(true);
    }

    public abstract Builder toBuilder();

    /** These settings with the given token, used to keep the stored token when a save omits it. */
    public SlurmSettings withToken(final String token) {
        return toBuilder().token(token).build();
    }

    /** The configured time limit in whole minutes, or null when none is set or it is unlimited. */
    @JsonIgnore
    @Nullable
    public Integer timeLimitMinutes() {
        return parseTimeLimitMinutes(timeLimit());
    }

    public List<String> validate() {
        final List<String> errors = new ArrayList<>();
        if (StringUtils.isBlank(restUrl())) {
            errors.add("Slurm REST URL cannot be empty for the Slurm backend");
        } else if (!isHttpUrl(restUrl())) {
            errors.add("Slurm REST URL must be an http or https URL");
        }
        if (StringUtils.isNotBlank(apiVersion()) && compareApiVersions(apiVersion(), MIN_API_VERSION) < 0) {
            errors.add("Slurm REST API version must be blank (detect automatically) or " + MIN_API_VERSION
                    + " or newer, e.g. " + MIN_API_VERSION);
        }
        if (StringUtils.isBlank(user())) {
            errors.add("Slurm user cannot be empty for the Slurm backend");
        }
        if (StringUtils.isBlank(token())) {
            errors.add("Slurm token cannot be empty for the Slurm backend");
        }
        if (!RUNTIMES.contains(runtime())) {
            errors.add("Slurm container runtime must be one of " + RUNTIMES);
        }
        if (!allowDockerPull() && StringUtils.isBlank(sifDirectory())) {
            errors.add("A SIF directory is required when pulling images from a registry is disabled");
        }
        if (StringUtils.isNotBlank(timeLimit())) {
            try {
                parseTimeLimitMinutes(timeLimit());
            } catch (IllegalArgumentException e) {
                errors.add(e.getMessage());
            }
        }
        return errors;
    }

    /**
     * Compares two API versions such as {@code v0.0.40}. A string that isn't a version sorts before every version,
     * so it fails a minimum-version check.
     */
    public static int compareApiVersions(@Nullable final String a, @Nullable final String b) {
        final int[] left = parseApiVersion(a);
        final int[] right = parseApiVersion(b);
        if (left == null || right == null) {
            return left == null ? (right == null ? 0 : -1) : 1;
        }
        for (int i = 0; i < 3; i++) {
            if (left[i] != right[i]) {
                return Integer.compare(left[i], right[i]);
            }
        }
        return 0;
    }

    @Nullable
    private static int[] parseApiVersion(@Nullable final String version) {
        if (version == null) {
            return null;
        }
        final Matcher m = API_VERSION_PATTERN.matcher(version.trim());
        if (!m.matches()) {
            return null;
        }
        try {
            return new int[]{Integer.parseInt(m.group(1)), Integer.parseInt(m.group(2)), Integer.parseInt(m.group(3))};
        } catch (NumberFormatException e) {
            return null;
        }
    }

    // minutes | minutes:seconds | hours:minutes:seconds | days-hours | days-hours:minutes | days-hours:minutes:seconds
    private static final Pattern TIME_MIN = Pattern.compile("^(\\d+)$");
    private static final Pattern TIME_MIN_SEC = Pattern.compile("^(\\d+):(\\d+)$");
    private static final Pattern TIME_H_M_S = Pattern.compile("^(\\d+):(\\d+):(\\d+)$");
    private static final Pattern TIME_DAYS = Pattern.compile("^(\\d+)-(\\d+)(?::(\\d+)(?::(\\d+))?)?$");

    /**
     * Parses a time limit in any of the formats sbatch's {@code --time} accepts into whole minutes, rounding
     * leftover seconds up so a job never gets less time than asked for.
     *
     * @return the limit in minutes, or null for a blank value or {@code UNLIMITED}/{@code INFINITE}
     * @throws IllegalArgumentException if the value is in none of those formats
     */
    @Nullable
    public static Integer parseTimeLimitMinutes(@Nullable final String timeLimit) {
        if (StringUtils.isBlank(timeLimit)) {
            return null;
        }
        final String value = timeLimit.trim();
        if ("UNLIMITED".equalsIgnoreCase(value) || "INFINITE".equalsIgnoreCase(value)) {
            return null;
        }
        long seconds;
        Matcher m;
        try {
            if ((m = TIME_MIN.matcher(value)).matches()) {
                seconds = Long.parseLong(m.group(1)) * 60;
            } else if ((m = TIME_MIN_SEC.matcher(value)).matches()) {
                seconds = Long.parseLong(m.group(1)) * 60 + Long.parseLong(m.group(2));
            } else if ((m = TIME_H_M_S.matcher(value)).matches()) {
                seconds = Long.parseLong(m.group(1)) * 3600 + Long.parseLong(m.group(2)) * 60 + Long.parseLong(m.group(3));
            } else if ((m = TIME_DAYS.matcher(value)).matches()) {
                seconds = Long.parseLong(m.group(1)) * 86400 + Long.parseLong(m.group(2)) * 3600
                        + (m.group(3) == null ? 0 : Long.parseLong(m.group(3)) * 60)
                        + (m.group(4) == null ? 0 : Long.parseLong(m.group(4)));
            } else {
                throw new IllegalArgumentException(invalidTimeLimit(value));
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(invalidTimeLimit(value));
        }
        final long minutes = (seconds + 59) / 60;
        if (minutes <= 0 || minutes > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(invalidTimeLimit(value));
        }
        return (int) minutes;
    }

    private static String invalidTimeLimit(final String value) {
        return "Slurm time limit \"" + value + "\" is not valid. Use minutes, minutes:seconds, hours:minutes:seconds, "
                + "days-hours, days-hours:minutes, days-hours:minutes:seconds or UNLIMITED.";
    }

    private static boolean isHttpUrl(final String url) {
        try {
            final URI uri = new URI(url);
            return ("http".equalsIgnoreCase(uri.getScheme()) || "https".equalsIgnoreCase(uri.getScheme()))
                    && StringUtils.isNotBlank(uri.getHost());
        } catch (URISyntaxException e) {
            return false;
        }
    }

    /** Masks the token so it can't leak through logging of the server configuration. */
    @Override
    public String toString() {
        return "SlurmSettings{restUrl=" + restUrl() + ", apiVersion=" + apiVersion() + ", user=" + user()
                + ", token=" + (tokenSet() ? "****" : "<none>") + ", partition=" + partition()
                + ", account=" + account() + ", qos=" + qos() + ", timeLimit=" + timeLimit()
                + ", runtime=" + runtime() + ", sifDirectory=" + sifDirectory()
                + ", allowDockerPull=" + allowDockerPull() + ", pullCacheDirectory=" + pullCacheDirectory() + "}";
    }

    @AutoValue.Builder
    public abstract static class Builder {
        public abstract Builder restUrl(String restUrl);
        public abstract Builder apiVersion(String apiVersion);
        public abstract Builder user(String user);
        public abstract Builder token(String token);
        public abstract Builder partition(String partition);
        public abstract Builder account(String account);
        public abstract Builder qos(String qos);
        public abstract Builder timeLimit(String timeLimit);
        public abstract Builder runtime(String runtime);
        public abstract Builder sifDirectory(String sifDirectory);
        public abstract Builder allowDockerPull(boolean allowDockerPull);
        public abstract Builder pullCacheDirectory(String pullCacheDirectory);
        public abstract SlurmSettings build();
    }
}
