package org.nrg.containers.api.slurm;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.MissingNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.nrg.containers.exceptions.ContainerBackendException;
import org.nrg.containers.model.server.docker.SlurmSettings;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.ResponseErrorHandler;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * slurmrestd client for API v0.0.40 and later.
 * <p>
 * The API version is resolved on first use from the server's OpenAPI description: the configured version if one is
 * set, otherwise the newest of {@link SlurmSettings#SUPPORTED_API_VERSIONS} the server offers. Responses are read as
 * JSON trees and every field is optional, so one parser covers all supported versions: numbers may arrive plain or
 * wrapped as {@code {set, infinite, number}}, and states as a single string or an array of base state and flags.
 * <p>
 * Requests never carry secrets in the URL, and neither the token nor job environments are logged.
 */
@Slf4j
public class SlurmClientImpl implements SlurmClient {
    static final int CONNECT_TIMEOUT_MS = 5_000;
    static final int READ_TIMEOUT_MS = 30_000;

    static final String HEADER_USER = "X-SLURM-USER-NAME";
    static final String HEADER_TOKEN = "X-SLURM-USER-TOKEN";

    private static final Pattern PING_PATH = Pattern.compile("^/slurm/(v\\d+\\.\\d+\\.\\d+)/ping/?$");
    private static final Pattern ACCOUNTING_JOB_PATH = Pattern.compile("^/slurmdb/(v\\d+\\.\\d+\\.\\d+)/job/\\{job_id}/?$");
    private static final int MAX_BODY_IN_MESSAGE = 300;

    /** {@code ESLURM_JOB_HELD}: the expected submit result for a job submitted held. */
    private static final int JOB_HELD = 2042;

    private static final ResponseErrorHandler NEVER_THROW = new ResponseErrorHandler() {
        @Override
        public boolean hasError(final ClientHttpResponse response) {
            return false;
        }

        @Override
        public void handleError(final ClientHttpResponse response) {
        }
    };

    private final SlurmSettings settings;
    private final SlurmCredentialProvider credentialProvider;
    private final RestTemplate restTemplate;
    private final String baseUrl;
    private final ObjectMapper mapper = new ObjectMapper();

    private volatile Endpoints endpoints;

    public SlurmClientImpl(final SlurmSettings settings) {
        this(settings, new ServiceAccountCredentialProvider(settings), defaultRestTemplate());
    }

    SlurmClientImpl(final SlurmSettings settings, final SlurmCredentialProvider credentialProvider, final RestTemplate restTemplate) {
        this.settings = settings;
        this.credentialProvider = credentialProvider;
        this.restTemplate = restTemplate;
        this.restTemplate.setErrorHandler(NEVER_THROW);
        this.baseUrl = StringUtils.stripEnd(StringUtils.trimToEmpty(settings.restUrl()), "/");
    }

    static RestTemplate defaultRestTemplate() {
        final SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        requestFactory.setConnectTimeout(CONNECT_TIMEOUT_MS);
        requestFactory.setReadTimeout(READ_TIMEOUT_MS);
        return new RestTemplate(requestFactory);
    }

    @Override
    public String apiVersion() throws ContainerBackendException {
        return endpoints().apiVersion;
    }

    @Override
    public SlurmPing ping() throws ContainerBackendException {
        final Endpoints endpoints = endpoints();
        final JsonNode json = call(HttpMethod.GET, endpoints.slurm("/ping"), null);

        final List<String> up = new ArrayList<>();
        final List<String> down = new ArrayList<>();
        for (final JsonNode ping : json.path("pings")) {
            final String host = StringUtils.defaultIfBlank(ping.path("hostname").asText(null), "unknown");
            // v0.0.40 reports pinged: "UP"/"DOWN"; later versions add a responding boolean.
            final boolean responding = ping.has("responding")
                    ? ping.path("responding").asBoolean(false)
                    : "UP".equalsIgnoreCase(ping.path("pinged").asText(""));
            (responding ? up : down).add(host);
        }

        final JsonNode slurm = json.path("meta").path("slurm");
        return SlurmPing.create(endpoints.apiVersion, textOrNull(slurm.path("release")), textOrNull(slurm.path("cluster")),
                up, down, endpoints.accounting, credentialProvider.tokenExpiresAt());
    }

    @Override
    public long submit(final SlurmJobRequest request) throws ContainerBackendException {
        final Endpoints endpoints = endpoints();
        final ObjectNode body = mapper.createObjectNode();
        final ObjectNode job = jobDescription(request);
        // The script goes in both places. v0.0.40 rejects a submission without the top-level field ("Populated
        // "script" field is required") and accepts job.script alongside it; later versions document the top-level
        // field as the same script as job.script.
        body.put("script", request.script());
        job.put("script", request.script());
        body.set("job", job);

        log.debug("Submitting Slurm job {}", request);
        final JsonNode json = call(HttpMethod.POST, endpoints.slurm("/job/submit"), body);

        Long jobId = longValue(json.get("job_id"));
        if (jobId == null) {
            jobId = longValue(json.path("result").get("job_id"));
        }
        if (jobId == null) {
            throw new ContainerBackendException("slurmrestd accepted job " + request.name() + " but returned no job ID");
        }

        final Long resultCode = longValue(json.path("result").get("error_code"));
        if (resultCode != null && resultCode != 0 && !(resultCode == JOB_HELD && request.hold())) {
            log.warn("Slurm job {} ({}) was submitted with result {}: {}", jobId, request.name(), resultCode,
                    textOrNull(json.path("result").path("error")));
        }

        final String userMessage = StringUtils.defaultIfBlank(textOrNull(json.path("job_submit_user_msg")),
                textOrNull(json.path("result").path("job_submit_user_msg")));
        if (StringUtils.isNotBlank(userMessage)) {
            log.info("Slurm job {} ({}): {}", jobId, request.name(), userMessage);
        }
        log.info("Submitted Slurm job {} ({}){}", jobId, request.name(), request.hold() ? ", held" : "");
        return jobId;
    }

    @Override
    public void release(final long jobId) throws ContainerBackendException {
        final ObjectNode body = mapper.createObjectNode();
        body.put("hold", false);
        final String path = endpoints().slurm("/job/" + jobId);
        final JsonNode json = call(HttpMethod.POST, path, body);
        checkResults(json.path("results"), HttpMethod.POST, path);
        log.info("Released Slurm job {}", jobId);
    }

    @Override
    public boolean cancel(final long jobId) throws ContainerBackendException {
        try {
            call(HttpMethod.DELETE, endpoints().slurm("/job/" + jobId), null);
            log.info("Cancelled Slurm job {}", jobId);
            return true;
        } catch (SlurmApiException e) {
            if (e.hasError(SlurmError.INVALID_JOB_ID) || e.hasError(SlurmError.ALREADY_DONE)) {
                log.debug("Nothing to cancel for Slurm job {}: {}", jobId, e.getMessage());
                return false;
            }
            throw e;
        }
    }

    @Override
    public Optional<SlurmJobState> getJob(final long jobId) throws ContainerBackendException {
        final Endpoints endpoints = endpoints();
        try {
            final JsonNode json = call(HttpMethod.GET, endpoints.slurm("/job/" + jobId), null);
            final Optional<SlurmJobState> state = lastMatching(json.path("jobs"), jobId, SlurmClientImpl::fromController);
            if (state.isPresent()) {
                return state;
            }
        } catch (SlurmApiException e) {
            if (!e.hasError(SlurmError.INVALID_JOB_ID) && e.getHttpStatus() != 404) {
                throw e;
            }
        }

        // slurmctld forgets jobs MinJobAge seconds after they end; slurmdbd keeps them.
        if (!endpoints.accounting) {
            log.debug("slurmctld does not know Slurm job {} and slurmdbd is not available to look it up", jobId);
            return Optional.empty();
        }
        try {
            final JsonNode json = call(HttpMethod.GET, endpoints.accounting("/job/" + jobId), null);
            return lastMatching(json.path("jobs"), jobId, SlurmClientImpl::fromAccounting);
        } catch (SlurmApiException e) {
            if (e.getHttpStatus() == 404 && e.getErrors().isEmpty()) {
                log.warn("slurmrestd does not serve the slurmdbd endpoints, so finished Slurm jobs cannot be looked up: {}", e.getMessage());
                endpoints.accounting = false;
                return Optional.empty();
            }
            throw e;
        }
    }

    @Override
    public SlurmJobSnapshot getJobs(@Nullable final Long changedSince) throws ContainerBackendException {
        final String path = endpoints().slurm("/jobs") + (changedSince == null ? "" : "?update_time=" + changedSince);
        final JsonNode json;
        try {
            json = call(HttpMethod.GET, path, null);
        } catch (SlurmApiException e) {
            if (e.hasError(SlurmError.NO_CHANGE_IN_DATA)) {
                return SlurmJobSnapshot.create(changedSince, ImmutableMap.of());
            }
            throw e;
        }

        final Map<Long, SlurmJobState> jobs = new LinkedHashMap<>();
        for (final JsonNode job : json.path("jobs")) {
            final SlurmJobState state = fromController(job);
            if (state != null) {
                jobs.put(state.jobId(), state);
            }
        }
        // slurmctld sends last_update 0 when nothing changed, so keep the caller's cursor then.
        final Long lastUpdate = epochSeconds(json.get("last_update"));
        return SlurmJobSnapshot.create(lastUpdate != null ? lastUpdate : changedSince, ImmutableMap.copyOf(jobs));
    }

    // ---------------------------------------------------------------------------------------------------------------
    // API version detection
    // ---------------------------------------------------------------------------------------------------------------

    private static final class Endpoints {
        private final String apiVersion;
        private volatile boolean accounting;

        private Endpoints(final String apiVersion, final boolean accounting) {
            this.apiVersion = apiVersion;
            this.accounting = accounting;
        }

        private String slurm(final String path) {
            return "/slurm/" + apiVersion + path;
        }

        private String accounting(final String path) {
            return "/slurmdb/" + apiVersion + path;
        }
    }

    private Endpoints endpoints() throws ContainerBackendException {
        Endpoints resolved = endpoints;
        if (resolved == null) {
            synchronized (this) {
                if (endpoints == null) {
                    endpoints = detectEndpoints();
                }
                resolved = endpoints;
            }
        }
        return resolved;
    }

    private Endpoints detectEndpoints() throws ContainerBackendException {
        final String pinned = settings.apiVersion();
        final JsonNode spec;
        try {
            spec = call(HttpMethod.GET, "/openapi/v3", null);
        } catch (SlurmApiException e) {
            if (pinned != null && !e.isAuthenticationFailure()) {
                // Assume accounting is there; getJob() turns it off if the endpoint turns out to be missing.
                log.warn("Could not read the slurmrestd OpenAPI description at {}, so using the configured API version {}: {}",
                        baseUrl, pinned, e.getMessage());
                return new Endpoints(pinned, true);
            }
            throw e;
        }

        final Set<String> offered = new TreeSet<>(SlurmSettings::compareApiVersions);
        final Set<String> withAccounting = new TreeSet<>(SlurmSettings::compareApiVersions);
        for (final Iterator<String> paths = spec.path("paths").fieldNames(); paths.hasNext(); ) {
            final String path = paths.next();
            final Matcher ping = PING_PATH.matcher(path);
            if (ping.matches()) {
                offered.add(ping.group(1));
            }
            final Matcher accounting = ACCOUNTING_JOB_PATH.matcher(path);
            if (accounting.matches()) {
                withAccounting.add(accounting.group(1));
            }
        }

        final String version;
        if (pinned != null) {
            if (!offered.contains(pinned)) {
                throw new ContainerBackendException("slurmrestd at " + baseUrl + " does not offer the configured API version "
                        + pinned + ". It offers " + offered + ". Clear the API version to detect it automatically.");
            }
            version = pinned;
        } else {
            version = SlurmSettings.SUPPORTED_API_VERSIONS.stream()
                    .filter(offered::contains)
                    .reduce((older, newer) -> newer)
                    .orElseThrow(() -> new ContainerBackendException("slurmrestd at " + baseUrl + " offers API versions "
                            + offered + ", but XNAT needs one of " + SlurmSettings.SUPPORTED_API_VERSIONS
                            + " (Slurm 23.11 or later)."));
        }

        final boolean accounting = withAccounting.contains(version);
        log.info("Using slurmrestd API version {} at {} (offered: {}; slurmdbd endpoints: {})",
                version, baseUrl, offered, accounting ? "yes" : "no");
        return new Endpoints(version, accounting);
    }

    // ---------------------------------------------------------------------------------------------------------------
    // HTTP
    // ---------------------------------------------------------------------------------------------------------------

    private JsonNode call(final HttpMethod method, final String path, @Nullable final JsonNode body) throws ContainerBackendException {
        final SlurmCredentials credentials = credentialProvider.credentials();
        final HttpHeaders headers = new HttpHeaders();
        headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));
        headers.set(HEADER_USER, credentials.user());
        headers.set(HEADER_TOKEN, credentials.token());
        final String content;
        if (body != null) {
            headers.setContentType(MediaType.APPLICATION_JSON);
            try {
                content = mapper.writeValueAsString(body);
            } catch (JsonProcessingException e) {
                throw new ContainerBackendException("Could not write the request body for " + method + " " + path, e);
            }
        } else {
            content = null;
        }

        final ResponseEntity<String> response;
        try {
            response = restTemplate.exchange(URI.create(baseUrl + path), method, new HttpEntity<>(content, headers), String.class);
        } catch (RestClientException | IllegalArgumentException e) {
            throw new ContainerBackendException("Could not reach slurmrestd at " + baseUrl + " for " + method + " " + path
                    + ": " + e.getMessage(), e);
        }

        final int status = response.getStatusCodeValue();
        final String text = response.getBody();
        JsonNode json = MissingNode.getInstance();
        if (StringUtils.isNotBlank(text)) {
            try {
                json = mapper.readTree(text);
            } catch (IOException e) {
                if (status < 300) {
                    throw new ContainerBackendException("slurmrestd returned a response that is not JSON for " + method + " " + path);
                }
                throw new SlurmApiException(describeFailure(method, path, status, StringUtils.abbreviate(text.trim(), MAX_BODY_IN_MESSAGE)),
                        status, Collections.emptyList());
            }
        }

        for (final JsonNode warning : json.path("warnings")) {
            // Routine: "found nothing", "No job changes since ...", "Job is in held state", "already sent signal".
            log.debug("slurmrestd warning for {} {}: {}", method, path,
                    StringUtils.defaultIfBlank(textOrNull(warning.path("description")), warning.toString()));
        }

        final List<SlurmError> errors = parseErrors(json.path("errors"));
        if (status >= 300 || !errors.isEmpty()) {
            final String detail = errors.isEmpty() ? null : errors.stream().map(SlurmError::toString).collect(Collectors.joining("; "));
            throw new SlurmApiException(describeFailure(method, path, status, detail), status, errors);
        }
        return json;
    }

    private String describeFailure(final HttpMethod method, final String path, final int status, @Nullable final String detail) {
        final StringBuilder message = new StringBuilder("slurmrestd returned HTTP ").append(status)
                .append(" for ").append(method).append(' ').append(path);
        if (StringUtils.isNotBlank(detail)) {
            message.append(": ").append(detail);
        }
        if (status == 401 || status == 403) {
            message.append(". Check the Slurm user and token");
            final Instant expiresAt = credentialProvider.tokenExpiresAt();
            if (expiresAt != null && expiresAt.isBefore(Instant.now())) {
                message.append("; the token expired at ").append(expiresAt);
            }
            message.append('.');
        }
        return message.toString();
    }

    private static List<SlurmError> parseErrors(final JsonNode errors) {
        final List<SlurmError> parsed = new ArrayList<>();
        for (final JsonNode error : errors) {
            final Long number = longValue(error.get("error_number"));
            final SlurmError entry = SlurmError.create(number == null ? null : number.intValue(), textOrNull(error.path("error")),
                    textOrNull(error.path("description")), textOrNull(error.path("source")));
            if (entry.errorNumber() != null || entry.error() != null || entry.description() != null) {
                parsed.add(entry);
            }
        }
        return parsed;
    }

    /** Job updates report a result per job; a failed update can still come back as HTTP 200. */
    private static void checkResults(final JsonNode results, final HttpMethod method, final String path) throws SlurmApiException {
        final List<SlurmError> errors = new ArrayList<>();
        for (final JsonNode result : results) {
            final Long code = longValue(result.get("error_code"));
            if (code != null && code != 0) {
                errors.add(SlurmError.create(code.intValue(), textOrNull(result.path("error")), textOrNull(result.path("why")), null));
            }
        }
        if (!errors.isEmpty()) {
            throw new SlurmApiException("slurmrestd could not complete " + method + " " + path + ": "
                    + errors.stream().map(SlurmError::toString).collect(Collectors.joining("; ")), 200, errors);
        }
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Request and response mapping
    // ---------------------------------------------------------------------------------------------------------------

    private ObjectNode jobDescription(final SlurmJobRequest request) {
        final ObjectNode job = mapper.createObjectNode();
        job.put("name", request.name());
        job.put("current_working_directory", request.workingDirectory());
        job.put("standard_output", request.standardOutput());
        if (request.standardError() != null) {
            job.put("standard_error", request.standardError());
        }
        final ArrayNode environment = job.putArray("environment");
        if (request.environment().isEmpty()) {
            environment.add(SlurmJobRequest.DEFAULT_PATH);
        } else {
            request.environment().forEach(environment::add);
        }
        putIfNotBlank(job, "partition", request.partition());
        putIfNotBlank(job, "account", request.account());
        putIfNotBlank(job, "qos", request.qos());
        putIfNotBlank(job, "tres_per_node", request.tresPerNode());
        putIfNotBlank(job, "comment", request.comment());
        if (request.timeLimitMinutes() != null) {
            job.set("time_limit", wrappedNumber(request.timeLimitMinutes()));
        }
        if (request.cpusPerTask() != null) {
            job.put("cpus_per_task", request.cpusPerTask());
        }
        if (request.memoryPerNodeMb() != null) {
            job.set("memory_per_node", wrappedNumber(request.memoryPerNodeMb()));
        }
        job.put("hold", request.hold());
        return job;
    }

    /** Every supported version accepts the {@code {set, infinite, number}} form for its no-value number fields. */
    private ObjectNode wrappedNumber(final long number) {
        final ObjectNode wrapped = mapper.createObjectNode();
        wrapped.put("set", true);
        wrapped.put("infinite", false);
        wrapped.put("number", number);
        return wrapped;
    }

    private static void putIfNotBlank(final ObjectNode node, final String field, @Nullable final String value) {
        if (StringUtils.isNotBlank(value)) {
            node.put(field, value);
        }
    }

    @Nullable
    static SlurmJobState fromController(final JsonNode job) {
        final Long jobId = longValue(job.get("job_id"));
        if (jobId == null) {
            return null;
        }
        final SlurmJobState.Builder builder = SlurmJobState.builder()
                .jobId(jobId)
                .source(SlurmJobState.Source.CONTROLLER)
                .nodes(textOrNull(job.path("nodes")))
                .submitTime(epochSeconds(job.get("submit_time")))
                .startTime(epochSeconds(job.get("start_time")))
                .endTime(epochSeconds(job.get("end_time")));
        applyStates(builder, job.get("job_state"));
        final String reason = reason(job.path("state_reason"));
        builder.reason(reason);
        builder.held(job.path("hold").asBoolean(false) || (reason != null && reason.startsWith("JobHeld")));
        applyExitCode(builder, job.get("exit_code"));
        return withoutOutcomeUnlessEnded(builder.build());
    }

    @Nullable
    static SlurmJobState fromAccounting(final JsonNode job) {
        final Long jobId = longValue(job.get("job_id"));
        if (jobId == null) {
            return null;
        }
        final JsonNode state = job.path("state");
        final String nodes = textOrNull(job.path("nodes"));
        final SlurmJobState.Builder builder = SlurmJobState.builder()
                .jobId(jobId)
                .source(SlurmJobState.Source.ACCOUNTING)
                .nodes("None assigned".equals(nodes) ? null : nodes)
                .submitTime(epochSeconds(job.path("time").get("submission")))
                .startTime(epochSeconds(job.path("time").get("start")))
                .endTime(epochSeconds(job.path("time").get("end")))
                .reason(reason(state.path("reason")));
        applyStates(builder, state.isObject() ? state.get("current") : state);
        applyExitCode(builder, job.get("exit_code"));
        return withoutOutcomeUnlessEnded(builder.build());
    }

    /**
     * Before a job ends, slurmctld reports exit code 0 with status SUCCESS, and an end time of start plus time
     * limit. Neither is an outcome yet, so drop them.
     */
    private static SlurmJobState withoutOutcomeUnlessEnded(final SlurmJobState state) {
        if (state.isTerminal()) {
            return state;
        }
        return state.toBuilder().exitCode(null).signal(null).endTime(null).build();
    }

    private static Optional<SlurmJobState> lastMatching(final JsonNode jobs, final long jobId, final Function<JsonNode, SlurmJobState> parser) {
        SlurmJobState match = null;
        for (final JsonNode job : jobs) {
            final SlurmJobState state = parser.apply(job);
            if (state != null && state.jobId() == jobId) {
                match = state;
            }
        }
        return Optional.ofNullable(match);
    }

    /** Splits Slurm's state list into the base state and its flags. */
    private static void applyStates(final SlurmJobState.Builder builder, @Nullable final JsonNode states) {
        final List<String> names = new ArrayList<>();
        if (states != null && states.isArray()) {
            states.forEach(s -> names.add(s.asText().trim().toUpperCase(Locale.ROOT)));
        } else if (states != null && states.isTextual()) {
            for (final String s : states.asText().split("[,+ ]+")) {
                names.add(s.trim().toUpperCase(Locale.ROOT));
            }
        }
        names.removeIf(String::isEmpty);
        final String base = names.stream().filter(SlurmJobState.BASE_STATES::contains).findFirst().orElse("UNKNOWN");
        names.remove(base);
        builder.state(base).flags(names);
    }

    private static void applyExitCode(final SlurmJobState.Builder builder, @Nullable final JsonNode exitCode) {
        if (exitCode == null || exitCode.isNull() || exitCode.isMissingNode()) {
            return;
        }
        if (!exitCode.isObject()) {
            final Long code = longValue(exitCode);
            builder.exitCode(code == null ? null : code.intValue());
            return;
        }
        final Set<String> status = new TreeSet<>();
        final JsonNode statusNode = exitCode.path("status");
        if (statusNode.isArray()) {
            statusNode.forEach(s -> status.add(s.asText()));
        } else if (statusNode.isTextual()) {
            status.add(statusNode.asText());
        }
        if (status.contains("PENDING") || status.contains("INVALID")) {
            return;
        }
        final Long code = longValue(exitCode.get("return_code"));
        builder.exitCode(code == null ? null : code.intValue());

        final JsonNode signal = exitCode.path("signal");
        final String name = textOrNull(signal.path("name"));
        final Long id = longValue(signal.get("id"));
        if (name != null) {
            builder.signal(name);
        } else if (id != null && id > 0) {
            builder.signal(String.valueOf(id));
        }
    }

    @Nullable
    private static String reason(final JsonNode reason) {
        final String text = textOrNull(reason);
        return text == null || "None".equals(text) ? null : text;
    }

    /** Slurm uses 0 for times that are not set yet. */
    @Nullable
    private static Long epochSeconds(@Nullable final JsonNode node) {
        final Long value = longValue(node);
        return value == null || value <= 0 ? null : value;
    }

    /**
     * Reads a number that may be plain or wrapped as {@code {set, infinite, number}}.
     *
     * @return null when the field is absent, unset or infinite
     */
    @Nullable
    static Long longValue(@Nullable final JsonNode node) {
        if (node == null || node.isNull() || node.isMissingNode()) {
            return null;
        }
        if (node.isNumber()) {
            return node.asLong();
        }
        if (node.isTextual()) {
            try {
                return Long.parseLong(node.asText().trim());
            } catch (NumberFormatException e) {
                return null;
            }
        }
        if (node.isObject()) {
            if (node.has("set") && !node.path("set").asBoolean(false)) {
                return null;
            }
            if (node.path("infinite").asBoolean(false)) {
                return null;
            }
            return longValue(node.get("number"));
        }
        return null;
    }

    @Nullable
    private static String textOrNull(final JsonNode node) {
        return node != null && node.isValueNode() && StringUtils.isNotBlank(node.asText()) ? node.asText() : null;
    }
}
