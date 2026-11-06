package org.nrg.containers.api.slurm;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;
import org.nrg.containers.exceptions.ContainerBackendException;
import org.nrg.containers.model.server.docker.SlurmSettings;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.ExpectedCount;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestTemplate;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.Base64;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.content;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.header;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.jsonPath;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withStatus;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

/** Response bodies follow the v0.0.40 schemas served by Slurm 23.11. */
public class SlurmClientImplTest {
    private static final String URL = "https://slurm.example.org:6820";
    private static final String TOKEN = "secret-token";

    private RestTemplate restTemplate;
    private MockRestServiceServer server;

    @Before
    public void setUp() {
        restTemplate = new RestTemplate();
        server = MockRestServiceServer.bindTo(restTemplate).build();
    }

    private SlurmClientImpl client() {
        return client(null);
    }

    private SlurmClientImpl client(@Nullable final String apiVersion) {
        final SlurmSettings settings = SlurmSettings.builder().restUrl(URL + "/").user("xnat").token(TOKEN)
                .apiVersion(apiVersion).partition("batch").timeLimit("2:00:00").build();
        return new SlurmClientImpl(settings, new ServiceAccountCredentialProvider(settings), restTemplate);
    }

    /** An OpenAPI description listing the ping path for each version, and the slurmdbd job path if asked. */
    private void expectOpenApi(final boolean accounting, final String... versions) {
        final String paths = Arrays.stream(versions)
                .map(v -> "\"/slurm/" + v + "/ping\":{}" + (accounting ? ",\"/slurmdb/" + v + "/job/{job_id}\":{}" : ""))
                .collect(Collectors.joining(","));
        server.expect(requestTo(URL + "/openapi/v3"))
                .andExpect(method(HttpMethod.GET))
                .andRespond(json("{\"openapi\":\"3.0.2\",\"paths\":{" + paths + "}}"));
    }

    private static org.springframework.test.web.client.ResponseCreator json(final String body) {
        return withSuccess(body, MediaType.APPLICATION_JSON);
    }

    private static org.springframework.test.web.client.ResponseCreator error(final HttpStatus status, final int number, final String description) {
        return withStatus(status).contentType(MediaType.APPLICATION_JSON)
                .body("{\"errors\":[{\"description\":\"" + description + "\",\"error_number\":" + number
                        + ",\"error\":\"" + description + "\",\"source\":\"test\"}],\"warnings\":[]}");
    }

    // ---------------------------------------------------------------------------------------------------------------
    // API version detection
    // ---------------------------------------------------------------------------------------------------------------

    @Test
    public void detectsTheNewestSupportedVersion() throws Exception {
        expectOpenApi(true, "v0.0.38", "v0.0.39", "v0.0.40");
        assertThat(client().apiVersion(), is("v0.0.40"));
        server.verify();
    }

    @Test
    public void ignoresVersionsNewerThanTheClientKnows() throws Exception {
        expectOpenApi(true, "v0.0.43", "v0.0.44", "v0.0.45", "v0.0.46");
        assertThat(client().apiVersion(), is("v0.0.45"));
    }

    @Test
    public void detectsTheVersionOnlyOnce() throws Exception {
        expectOpenApi(true, "v0.0.40");
        final SlurmClientImpl client = client();
        client.apiVersion();
        client.apiVersion();
        server.verify();
    }

    @Test
    public void serversOlderThanTheMinimumAreRejected() {
        expectOpenApi(true, "v0.0.38", "v0.0.39");
        try {
            client().apiVersion();
            fail("Expected detection to fail");
        } catch (ContainerBackendException e) {
            assertThat(e.getMessage(), containsString("[v0.0.38, v0.0.39]"));
            assertThat(e.getMessage(), containsString("Slurm 23.11 or later"));
        }
    }

    @Test
    public void aPinnedVersionMustBeOffered() {
        expectOpenApi(true, "v0.0.38", "v0.0.39", "v0.0.40");
        try {
            client("v0.0.42").apiVersion();
            fail("Expected the pinned version to be rejected");
        } catch (ContainerBackendException e) {
            assertThat(e.getMessage(), containsString("does not offer the configured API version v0.0.42"));
        }
    }

    @Test
    public void aPinnedVersionIsUsedWhenTheOpenApiDescriptionIsUnavailable() throws Exception {
        server.expect(requestTo(URL + "/openapi/v3")).andRespond(withStatus(HttpStatus.NOT_FOUND).body("Unable find requested URL"));
        assertThat(client("v0.0.40").apiVersion(), is("v0.0.40"));
    }

    @Test
    public void detectionIsRetriedAfterAFailure() throws Exception {
        server.expect(requestTo(URL + "/openapi/v3")).andRespond(withStatus(HttpStatus.SERVICE_UNAVAILABLE));
        expectOpenApi(true, "v0.0.40");
        final SlurmClientImpl client = client();
        try {
            client.apiVersion();
            fail("Expected the first detection to fail");
        } catch (ContainerBackendException expected) {
            // next call tries again
        }
        assertThat(client.apiVersion(), is("v0.0.40"));
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Authentication and errors
    // ---------------------------------------------------------------------------------------------------------------

    @Test
    public void everyRequestCarriesTheServiceAccountCredentials() throws Exception {
        server.expect(requestTo(URL + "/openapi/v3"))
                .andExpect(header(SlurmClientImpl.HEADER_USER, "xnat"))
                .andExpect(header(SlurmClientImpl.HEADER_TOKEN, TOKEN))
                .andRespond(json("{\"paths\":{\"/slurm/v0.0.40/ping\":{}}}"));
        client().apiVersion();
        server.verify();
    }

    @Test
    public void authenticationFailuresSayWhatToCheckWithoutTheToken() {
        server.expect(requestTo(URL + "/openapi/v3")).andRespond(withStatus(HttpStatus.UNAUTHORIZED).body("Authentication failure"));
        try {
            client().apiVersion();
            fail("Expected an authentication failure");
        } catch (ContainerBackendException e) {
            assertThat(e, org.hamcrest.Matchers.instanceOf(SlurmApiException.class));
            assertThat(((SlurmApiException) e).isAuthenticationFailure(), is(true));
            assertThat(e.getMessage(), containsString("Check the Slurm user and token"));
            assertThat(e.getMessage(), not(containsString(TOKEN)));
        }
    }

    @Test
    public void slurmErrorsAreReportedEvenWithHttp200() throws Exception {
        expectOpenApi(true, "v0.0.40");
        server.expect(requestTo(URL + "/slurm/v0.0.40/job/submit"))
                .andRespond(json("{\"errors\":[{\"description\":\"Invalid partition name specified\",\"error_number\":2015,"
                        + "\"error\":\"Invalid partition name specified\",\"source\":\"slurm_submit_batch_job()\"}]}"));
        try {
            client().submit(request().build());
            fail("Expected the submission to fail");
        } catch (SlurmApiException e) {
            assertThat(e.getMessage(), containsString("Invalid partition name specified"));
            assertThat(e.getMessage(), containsString("[error 2015]"));
            assertThat(e.hasError(2015), is(true));
        }
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Ping
    // ---------------------------------------------------------------------------------------------------------------

    @Test
    public void pingReportsControllersAndRelease() throws Exception {
        expectOpenApi(true, "v0.0.40");
        server.expect(requestTo(URL + "/slurm/v0.0.40/ping"))
                .andRespond(json("{\"pings\":[{\"hostname\":\"ctl1\",\"pinged\":\"UP\",\"latency\":120,\"mode\":\"primary\"},"
                        + "{\"hostname\":\"ctl2\",\"pinged\":\"DOWN\",\"latency\":0,\"mode\":\"backup\"}],"
                        + "\"meta\":{\"slurm\":{\"version\":{\"major\":\"23\",\"micro\":\"4\",\"minor\":\"11\"},"
                        + "\"release\":\"23.11.4\",\"cluster\":\"hpc\"}},\"errors\":[],\"warnings\":[]}"));

        final SlurmPing ping = client().ping();

        assertThat(ping.ok(), is(true));
        assertThat(ping.apiVersion(), is("v0.0.40"));
        assertThat(ping.release(), is("23.11.4"));
        assertThat(ping.cluster(), is("hpc"));
        assertThat(ping.controllersUp(), contains("ctl1"));
        assertThat(ping.controllersDown(), contains("ctl2"));
        assertThat(ping.accountingAvailable(), is(true));
    }

    @Test
    public void pingIsNotOkWhenNoControllerAnswers() throws Exception {
        expectOpenApi(false, "v0.0.40");
        server.expect(requestTo(URL + "/slurm/v0.0.40/ping"))
                .andRespond(json("{\"pings\":[{\"hostname\":\"ctl1\",\"pinged\":\"DOWN\"}]}"));
        final SlurmPing ping = client().ping();
        assertThat(ping.ok(), is(false));
        assertThat(ping.accountingAvailable(), is(false));
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Submit, release and cancel
    // ---------------------------------------------------------------------------------------------------------------

    private static SlurmJobRequest.Builder request() {
        return SlurmJobRequest.builder(SlurmSettings.builder().partition("batch").timeLimit("2:00:00").build())
                .name("xnat-42")
                .script("#!/bin/bash\necho hello\n")
                .workingDirectory("/shared/build/42")
                .standardOutput("/shared/build/42/stdout.log");
    }

    @Test
    public void submitSendsAHeldJobWithWrappedNumbersAndATopLevelScriptForV0040() throws Exception {
        expectOpenApi(true, "v0.0.40");
        server.expect(requestTo(URL + "/slurm/v0.0.40/job/submit"))
                .andExpect(method(HttpMethod.POST))
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.job.name").value("xnat-42"))
                .andExpect(jsonPath("$.script").value("#!/bin/bash\necho hello\n"))
                .andExpect(jsonPath("$.job.script").doesNotExist())
                .andExpect(jsonPath("$.job.current_working_directory").value("/shared/build/42"))
                .andExpect(jsonPath("$.job.standard_output").value("/shared/build/42/stdout.log"))
                .andExpect(jsonPath("$.job.standard_error").doesNotExist())
                .andExpect(jsonPath("$.job.hold").value(true))
                .andExpect(jsonPath("$.job.partition").value("batch"))
                .andExpect(jsonPath("$.job.account").doesNotExist())
                .andExpect(jsonPath("$.job.time_limit.set").value(true))
                .andExpect(jsonPath("$.job.time_limit.number").value(120))
                .andExpect(jsonPath("$.job.memory_per_node.number").value(4096))
                .andExpect(jsonPath("$.job.cpus_per_task").value(2))
                .andExpect(jsonPath("$.job.environment[0]").value("APPTAINERENV_FOO=bar"))
                .andRespond(json("{\"job_id\":1234,\"step_id\":\"batch\",\"job_submit_user_msg\":\"\","
                        + "\"result\":{\"job_id\":1234,\"step_id\":\"batch\",\"error_code\":0,\"error\":\"No error\"},"
                        + "\"errors\":[],\"warnings\":[]}"));

        final long jobId = client().submit(request().cpusPerTask(2).memoryPerNodeMb(4096L)
                .addEnvironment("APPTAINERENV_FOO", "bar").build());

        assertThat(jobId, is(1234L));
        server.verify();
    }

    @Test
    public void laterVersionsTakeTheScriptInsideTheJob() throws Exception {
        expectOpenApi(true, "v0.0.40", "v0.0.41");
        server.expect(requestTo(URL + "/slurm/v0.0.41/job/submit"))
                .andExpect(jsonPath("$.script").doesNotExist())
                .andExpect(jsonPath("$.job.script").value("#!/bin/bash\necho hello\n"))
                .andRespond(json("{\"job_id\":8}"));
        assertThat(client().submit(request().build()), is(8L));
    }

    @Test
    public void submitAlwaysSendsAnEnvironment() throws Exception {
        expectOpenApi(true, "v0.0.40");
        server.expect(requestTo(URL + "/slurm/v0.0.40/job/submit"))
                .andExpect(jsonPath("$.job.environment[0]").value(SlurmJobRequest.DEFAULT_PATH))
                .andRespond(json("{\"result\":{\"job_id\":7}}"));
        assertThat(client().submit(request().build()), is(7L));
    }

    @Test
    public void releaseClearsTheHold() throws Exception {
        expectOpenApi(true, "v0.0.40");
        server.expect(requestTo(URL + "/slurm/v0.0.40/job/1234"))
                .andExpect(method(HttpMethod.POST))
                .andExpect(jsonPath("$.hold").value(false))
                .andExpect(jsonPath("$.*").value(org.hamcrest.Matchers.hasSize(1)))
                .andRespond(json("{\"results\":[{\"job_id\":1234,\"step_id\":\"batch\",\"error\":\"No error\",\"error_code\":0,\"why\":\"\"}],"
                        + "\"job_id\":\"1234\",\"errors\":[],\"warnings\":[]}"));
        client().release(1234);
        server.verify();
    }

    @Test
    public void aFailedReleaseResultIsAnError() {
        expectOpenApi(true, "v0.0.40");
        server.expect(requestTo(URL + "/slurm/v0.0.40/job/1234"))
                .andRespond(json("{\"results\":[{\"job_id\":1234,\"error\":\"Access/permission denied\",\"error_code\":2002,"
                        + "\"why\":\"Job held by admin\"}],\"errors\":[]}"));
        try {
            client().release(1234);
            fail("Expected the release to fail");
        } catch (ContainerBackendException e) {
            assertThat(e.getMessage(), containsString("Job held by admin"));
        }
    }

    @Test
    public void cancelReportsWhetherThereWasAnythingToCancel() throws Exception {
        expectOpenApi(true, "v0.0.40");
        server.expect(requestTo(URL + "/slurm/v0.0.40/job/1")).andExpect(method(HttpMethod.DELETE))
                .andRespond(json("{\"errors\":[],\"warnings\":[]}"));
        server.expect(requestTo(URL + "/slurm/v0.0.40/job/2")).andExpect(method(HttpMethod.DELETE))
                .andRespond(error(HttpStatus.INTERNAL_SERVER_ERROR, SlurmError.ALREADY_DONE, "Job/step already completing or completed"));
        server.expect(requestTo(URL + "/slurm/v0.0.40/job/3")).andExpect(method(HttpMethod.DELETE))
                .andRespond(error(HttpStatus.INTERNAL_SERVER_ERROR, SlurmError.INVALID_JOB_ID, "Invalid job id specified"));

        final SlurmClientImpl client = client();
        assertThat(client.cancel(1), is(true));
        assertThat(client.cancel(2), is(false));
        assertThat(client.cancel(3), is(false));
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Job state
    // ---------------------------------------------------------------------------------------------------------------

    private static final String RUNNING_JOB = "{\"job_id\":1234,\"job_state\":[\"RUNNING\"],\"state_reason\":\"None\",\"hold\":false,"
            + "\"nodes\":\"node01\",\"start_time\":{\"set\":true,\"infinite\":false,\"number\":1700000000},"
            + "\"end_time\":{\"set\":true,\"infinite\":false,\"number\":1700003600},"
            + "\"exit_code\":{\"status\":[\"PENDING\"],\"return_code\":{\"set\":false,\"infinite\":false,\"number\":0}}}";

    @Test
    public void readsARunningJobFromTheController() throws Exception {
        expectOpenApi(true, "v0.0.40");
        server.expect(requestTo(URL + "/slurm/v0.0.40/job/1234"))
                .andRespond(json("{\"jobs\":[" + RUNNING_JOB + "],\"errors\":[],\"warnings\":[]}"));

        final SlurmJobState state = client().getJob(1234).get();

        assertThat(state.phase(), is(SlurmJobState.Phase.RUNNING));
        assertThat(state.source(), is(SlurmJobState.Source.CONTROLLER));
        assertThat(state.reason(), is(nullValue()));
        assertThat(state.exitCode(), is(nullValue()));
        assertThat(state.nodes(), is("node01"));
        assertThat(state.startTime(), is(1700000000L));
    }

    @Test
    public void readsStateFlagsHoldsAndExitCodes() throws Exception {
        expectOpenApi(true, "v0.0.40");
        server.expect(requestTo(URL + "/slurm/v0.0.40/job/5"))
                .andRespond(json("{\"jobs\":[{\"job_id\":5,\"job_state\":[\"PENDING\"],\"state_reason\":\"JobHeldUser\",\"hold\":true}]}"));
        server.expect(requestTo(URL + "/slurm/v0.0.40/job/6"))
                .andRespond(json("{\"jobs\":[{\"job_id\":6,\"job_state\":[\"FAILED\",\"COMPLETING\"],\"state_reason\":\"NonZeroExitCode\","
                        + "\"exit_code\":{\"status\":[\"ERROR\"],\"return_code\":{\"set\":true,\"infinite\":false,\"number\":3},"
                        + "\"signal\":{\"id\":{\"set\":false,\"infinite\":false,\"number\":0},\"name\":\"\"}}}]}"));
        server.expect(requestTo(URL + "/slurm/v0.0.40/job/7"))
                .andRespond(json("{\"jobs\":[{\"job_id\":7,\"job_state\":[\"CANCELLED\"],"
                        + "\"exit_code\":{\"status\":[\"SIGNALED\"],\"return_code\":{\"set\":true,\"infinite\":false,\"number\":0},"
                        + "\"signal\":{\"id\":{\"set\":true,\"infinite\":false,\"number\":15},\"name\":\"TERM\"}}}]}"));

        final SlurmClientImpl client = client();
        final SlurmJobState held = client.getJob(5).get();
        assertThat(held.phase(), is(SlurmJobState.Phase.PENDING));
        assertThat(held.held(), is(true));

        final SlurmJobState failed = client.getJob(6).get();
        assertThat(failed.phase(), is(SlurmJobState.Phase.FAILED));
        assertThat(failed.isCompleting(), is(true));
        assertThat(failed.flags(), contains("COMPLETING"));
        assertThat(failed.exitCode(), is(3));
        assertThat(failed.signal(), is(nullValue()));
        assertThat(failed.reason(), is("NonZeroExitCode"));

        final SlurmJobState cancelled = client.getJob(7).get();
        assertThat(cancelled.phase(), is(SlurmJobState.Phase.CANCELLED));
        assertThat(cancelled.signal(), is("TERM"));
    }

    @Test
    public void fallsBackToAccountingWhenTheControllerHasPurgedTheJob() throws Exception {
        expectOpenApi(true, "v0.0.40");
        server.expect(requestTo(URL + "/slurm/v0.0.40/job/1234"))
                .andRespond(error(HttpStatus.INTERNAL_SERVER_ERROR, SlurmError.INVALID_JOB_ID, "Invalid job id specified"));
        server.expect(requestTo(URL + "/slurmdb/v0.0.40/job/1234"))
                .andRespond(json("{\"jobs\":[{\"job_id\":1234,\"name\":\"xnat-42\",\"nodes\":\"node02\","
                        + "\"state\":{\"current\":[\"COMPLETED\"],\"reason\":\"None\"},"
                        + "\"exit_code\":{\"status\":[\"SUCCESS\"],\"return_code\":{\"set\":true,\"infinite\":false,\"number\":0}},"
                        + "\"time\":{\"start\":1700000000,\"end\":1700000100,\"elapsed\":100}}],\"errors\":[],\"warnings\":[]}"));

        final SlurmJobState state = client().getJob(1234).get();

        assertThat(state.source(), is(SlurmJobState.Source.ACCOUNTING));
        assertThat(state.phase(), is(SlurmJobState.Phase.SUCCEEDED));
        assertThat(state.exitCode(), is(0));
        assertThat(state.nodes(), is("node02"));
        assertThat(state.endTime(), is(1700000100L));
        server.verify();
    }

    @Test
    public void accountingUsesTheLatestRecordForARequeuedJob() throws Exception {
        expectOpenApi(true, "v0.0.40");
        server.expect(requestTo(URL + "/slurm/v0.0.40/job/9"))
                .andRespond(error(HttpStatus.NOT_FOUND, SlurmError.INVALID_JOB_ID, "Invalid job id specified"));
        server.expect(requestTo(URL + "/slurmdb/v0.0.40/job/9"))
                .andRespond(json("{\"jobs\":[{\"job_id\":9,\"state\":{\"current\":[\"NODE_FAIL\"]}},"
                        + "{\"job_id\":9,\"state\":{\"current\":[\"COMPLETED\"]}}]}"));
        assertThat(client().getJob(9).get().phase(), is(SlurmJobState.Phase.SUCCEEDED));
    }

    @Test
    public void aPurgedJobIsUnknownWithoutAccounting() throws Exception {
        expectOpenApi(false, "v0.0.40");
        server.expect(requestTo(URL + "/slurm/v0.0.40/job/1234"))
                .andRespond(error(HttpStatus.INTERNAL_SERVER_ERROR, SlurmError.INVALID_JOB_ID, "Invalid job id specified"));
        assertThat(client().getJob(1234), is(Optional.empty()));
        server.verify();
    }

    @Test
    public void aMissingAccountingEndpointTurnsTheFallbackOff() throws Exception {
        server.expect(requestTo(URL + "/openapi/v3")).andRespond(withStatus(HttpStatus.NOT_FOUND));
        server.expect(ExpectedCount.twice(), requestTo(URL + "/slurm/v0.0.40/job/1"))
                .andRespond(error(HttpStatus.INTERNAL_SERVER_ERROR, SlurmError.INVALID_JOB_ID, "Invalid job id specified"));
        server.expect(ExpectedCount.once(), requestTo(URL + "/slurmdb/v0.0.40/job/1"))
                .andRespond(withStatus(HttpStatus.NOT_FOUND).body("Unable find requested URL"));

        final SlurmClientImpl client = client("v0.0.40");
        assertThat(client.getJob(1), is(Optional.empty()));
        assertThat(client.getJob(1), is(Optional.empty()));
        server.verify();
    }

    @Test
    public void otherControllerErrorsAreNotMistakenForAPurgedJob() {
        expectOpenApi(true, "v0.0.40");
        server.expect(requestTo(URL + "/slurm/v0.0.40/job/1234"))
                .andRespond(error(HttpStatus.INTERNAL_SERVER_ERROR, 1007, "Protocol authentication error"));
        try {
            client().getJob(1234);
            fail("Expected the error to propagate");
        } catch (ContainerBackendException e) {
            assertThat(e.getMessage(), containsString("Protocol authentication error"));
        }
    }

    @Test
    public void getJobsReturnsChangedJobsAndTheNextCursor() throws Exception {
        expectOpenApi(true, "v0.0.40");
        server.expect(requestTo(URL + "/slurm/v0.0.40/jobs?update_time=1699999999"))
                .andRespond(json("{\"jobs\":[" + RUNNING_JOB + ",{\"job_id\":1235,\"job_state\":[\"COMPLETED\"]}],"
                        + "\"last_update\":{\"set\":true,\"infinite\":false,\"number\":1700000500},\"errors\":[],\"warnings\":[]}"));

        final SlurmJobSnapshot snapshot = client().getJobs(1699999999L);

        assertThat(snapshot.lastUpdate(), is(1700000500L));
        assertThat(snapshot.jobs().keySet(), contains(1234L, 1235L));
        assertThat(snapshot.jobs().get(1235L).phase(), is(SlurmJobState.Phase.SUCCEEDED));
    }

    @Test
    public void getJobsTreatsNoChangeAsEmpty() throws Exception {
        expectOpenApi(true, "v0.0.40");
        server.expect(requestTo(URL + "/slurm/v0.0.40/jobs?update_time=1700000500"))
                .andRespond(error(HttpStatus.INTERNAL_SERVER_ERROR, SlurmError.NO_CHANGE_IN_DATA, "Data has not changed since time specified"));
        final SlurmJobSnapshot snapshot = client().getJobs(1700000500L);
        assertThat(snapshot.lastUpdate(), is(1700000500L));
        assertThat(snapshot.jobs().entrySet(), is(empty()));
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Parsing helpers and value types
    // ---------------------------------------------------------------------------------------------------------------

    @Test
    public void numbersMayBePlainOrWrapped() throws Exception {
        final ObjectMapper mapper = new ObjectMapper();
        assertThat(SlurmClientImpl.longValue(mapper.readTree("42")), is(42L));
        assertThat(SlurmClientImpl.longValue(mapper.readTree("\"42\"")), is(42L));
        assertThat(SlurmClientImpl.longValue(mapper.readTree("{\"set\":true,\"infinite\":false,\"number\":42}")), is(42L));
        assertThat(SlurmClientImpl.longValue(mapper.readTree("{\"set\":false,\"infinite\":false,\"number\":0}")), is(nullValue()));
        assertThat(SlurmClientImpl.longValue(mapper.readTree("{\"set\":true,\"infinite\":true,\"number\":0}")), is(nullValue()));
        assertThat(SlurmClientImpl.longValue(mapper.readTree("null")), is(nullValue()));
        assertThat(SlurmClientImpl.longValue(null), is(nullValue()));
    }

    @Test
    public void statesMayBeAStringOrAnArray() throws Exception {
        final ObjectMapper mapper = new ObjectMapper();
        final SlurmJobState fromString = SlurmClientImpl.fromController(mapper.readTree("{\"job_id\":1,\"job_state\":\"COMPLETED\"}"));
        assertThat(fromString.phase(), is(SlurmJobState.Phase.SUCCEEDED));

        final SlurmJobState requeued = SlurmClientImpl.fromController(mapper.readTree("{\"job_id\":1,\"job_state\":[\"REQUEUED\",\"PENDING\"]}"));
        assertThat(requeued.state(), is("PENDING"));
        assertThat(requeued.flags(), contains("REQUEUED"));

        final SlurmJobState unknown = SlurmClientImpl.fromController(mapper.readTree("{\"job_id\":1,\"job_state\":[\"SOMETHING_NEW\"]}"));
        assertThat(unknown.phase(), is(SlurmJobState.Phase.UNKNOWN));
        assertThat(unknown.isTerminal(), is(false));
    }

    @Test
    public void everyFailureStateIsTerminal() {
        for (final String state : new String[]{"FAILED", "TIMEOUT", "NODE_FAIL", "PREEMPTED", "BOOT_FAIL", "DEADLINE", "OUT_OF_MEMORY"}) {
            final SlurmJobState job = SlurmJobState.builder().jobId(1).state(state).source(SlurmJobState.Source.CONTROLLER).build();
            assertThat(state, job.phase(), is(SlurmJobState.Phase.FAILED));
            assertThat(state, job.isTerminal(), is(true));
        }
    }

    @Test
    public void tokenExpiryIsReadFromTheJwt() {
        final String claims = Base64.getUrlEncoder().withoutPadding()
                .encodeToString("{\"exp\":1700000000,\"sun\":\"xnat\"}".getBytes(StandardCharsets.UTF_8));
        assertThat(ServiceAccountCredentialProvider.tokenExpiry("eyJhbGciOiJIUzI1NiJ9." + claims + ".sig"),
                is(Instant.ofEpochSecond(1700000000L)));
        assertThat(ServiceAccountCredentialProvider.tokenExpiry("not-a-jwt"), is(nullValue()));
        assertThat(ServiceAccountCredentialProvider.tokenExpiry(null), is(nullValue()));
    }

    @Test
    public void requestsAndCredentialsHideSecretsInToString() {
        final SlurmJobRequest request = request().addEnvironment("APPTAINERENV_PASSWORD", "hunter2").build();
        assertThat(request.toString(), containsString("APPTAINERENV_PASSWORD"));
        assertThat(request.toString(), not(containsString("hunter2")));
        assertThat(request.toString(), not(containsString("echo hello")));
        assertThat(SlurmCredentials.create("xnat", TOKEN).toString(), not(containsString(TOKEN)));
    }

    @Test
    public void scriptsMustStartWithAShebang() {
        try {
            request().script("echo hello").build();
            fail("Expected a script without #! to be rejected");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("#!"));
        }
    }
}
