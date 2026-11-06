package org.nrg.containers.api.slurm;

import org.junit.Before;
import org.junit.Test;
import org.nrg.containers.model.server.docker.SlurmSettings;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.test.web.client.ResponseCreator;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withStatus;

/**
 * Replays responses recorded from slurmrestd 23.11.4 (API v0.0.40), with the hostname replaced by {@code node01}.
 * The HTTP statuses are the ones that server returned.
 */
public class SlurmClientRealResponsesTest {
    private static final String URL = "http://slurm.example.org:6820";
    private static final String V = "/slurm/v0.0.40";

    private RestTemplate restTemplate;
    private MockRestServiceServer server;
    private SlurmClientImpl client;

    @Before
    public void setUp() {
        restTemplate = new RestTemplate();
        server = MockRestServiceServer.bindTo(restTemplate).build();
        final SlurmSettings settings = SlurmSettings.builder().restUrl(URL).user("xnat").token("t").build();
        client = new SlurmClientImpl(settings, new ServiceAccountCredentialProvider(settings), restTemplate);
        server.expect(requestTo(URL + "/openapi/v3")).andRespond(withStatus(HttpStatus.OK).contentType(MediaType.APPLICATION_JSON)
                .body("{\"paths\":{\"/slurm/v0.0.38/ping\":{},\"/slurm/v0.0.39/ping\":{},\"/slurm/v0.0.40/ping\":{},"
                        + "\"/slurmdb/v0.0.40/job/{job_id}\":{}}}"));
    }

    private static ResponseCreator recorded(final HttpStatus status, final String name) throws IOException {
        try (InputStream in = SlurmClientRealResponsesTest.class.getResourceAsStream("/slurm/v0.0.40/" + name + ".json")) {
            return withStatus(status).contentType(MediaType.APPLICATION_JSON).body(new String(in.readAllBytes(), StandardCharsets.UTF_8));
        }
    }

    @Test
    public void ping() throws Exception {
        server.expect(requestTo(URL + V + "/ping")).andRespond(recorded(HttpStatus.OK, "ping"));
        final SlurmPing ping = client.ping();
        assertThat(ping.ok(), is(true));
        assertThat(ping.controllersUp(), contains("node01"));
        assertThat(ping.release(), is("23.11.4"));
        assertThat(ping.cluster(), is("xnat"));
        assertThat(ping.accountingAvailable(), is(true));
    }

    @Test
    public void heldSubmissionReturnsTheJobId() throws Exception {
        server.expect(requestTo(URL + V + "/job/submit")).andRespond(recorded(HttpStatus.OK, "submit-held"));
        assertThat(client.submit(SlurmJobRequest.builder().name("xnat-probe").script("#!/bin/bash\nsleep 60\n")
                .workingDirectory("/tmp").standardOutput("/tmp/xnat-probe-%j.out").build()), is(4L));
    }

    @Test
    public void heldJob() throws Exception {
        server.expect(requestTo(URL + V + "/job/4")).andRespond(recorded(HttpStatus.OK, "job-held"));
        final SlurmJobState state = client.getJob(4).get();
        assertThat(state.phase(), is(SlurmJobState.Phase.PENDING));
        assertThat(state.held(), is(true));
        assertThat(state.reason(), is("JobHeldUser"));
        // slurmctld reports exit code 0 / SUCCESS for a job that has not run; that is not an outcome.
        assertThat(state.exitCode(), is(nullValue()));
        assertThat(state.startTime(), is(nullValue()));
        assertThat(state.endTime(), is(nullValue()));
    }

    @Test
    public void releaseThenRunning() throws Exception {
        server.expect(requestTo(URL + V + "/job/4")).andExpect(method(HttpMethod.POST)).andRespond(recorded(HttpStatus.OK, "release"));
        server.expect(requestTo(URL + V + "/job/4")).andExpect(method(HttpMethod.GET)).andRespond(recorded(HttpStatus.OK, "job-running"));

        client.release(4);
        final SlurmJobState state = client.getJob(4).get();

        assertThat(state.phase(), is(SlurmJobState.Phase.RUNNING));
        assertThat(state.held(), is(false));
        assertThat(state.reason(), is(nullValue()));
        assertThat(state.nodes(), is("node01"));
        assertThat(state.startTime(), is(1794001384L));
        // The reported end time is start plus time limit, a forecast rather than an end.
        assertThat(state.endTime(), is(nullValue()));
        assertThat(state.exitCode(), is(nullValue()));
    }

    @Test
    public void cancelledJob() throws Exception {
        server.expect(requestTo(URL + V + "/job/4")).andExpect(method(HttpMethod.DELETE)).andRespond(recorded(HttpStatus.OK, "cancel"));
        server.expect(requestTo(URL + V + "/job/4")).andExpect(method(HttpMethod.GET)).andRespond(recorded(HttpStatus.OK, "job-cancelled"));
        server.expect(requestTo(URL + V + "/job/4")).andExpect(method(HttpMethod.DELETE)).andRespond(recorded(HttpStatus.OK, "cancel-again"));

        assertThat(client.cancel(4), is(true));
        final SlurmJobState state = client.getJob(4).get();
        assertThat(state.phase(), is(SlurmJobState.Phase.CANCELLED));
        assertThat(state.signal(), is("TERM"));
        assertThat(state.exitCode(), is(nullValue()));
        assertThat(state.endTime(), is(1794001387L));
        assertThat(client.cancel(4), is(true));
    }

    @Test
    public void purgedJobComesFromAccounting() throws Exception {
        server.expect(requestTo(URL + V + "/job/4")).andRespond(recorded(HttpStatus.NOT_FOUND, "job-unknown"));
        server.expect(requestTo(URL + "/slurmdb/v0.0.40/job/4")).andRespond(recorded(HttpStatus.OK, "accounting-job-cancelled"));

        final SlurmJobState state = client.getJob(4).get();

        assertThat(state.source(), is(SlurmJobState.Source.ACCOUNTING));
        assertThat(state.phase(), is(SlurmJobState.Phase.CANCELLED));
        assertThat(state.startTime(), is(1794001384L));
        assertThat(state.endTime(), is(1794001387L));
    }

    @Test
    public void unknownJob() throws Exception {
        server.expect(requestTo(URL + V + "/job/67000000")).andRespond(recorded(HttpStatus.NOT_FOUND, "job-unknown"));
        server.expect(requestTo(URL + "/slurmdb/v0.0.40/job/67000000")).andRespond(recorded(HttpStatus.OK, "accounting-job-unknown"));
        server.expect(requestTo(URL + V + "/job/67000000")).andExpect(method(HttpMethod.DELETE))
                .andRespond(recorded(HttpStatus.NOT_FOUND, "cancel-unknown"));

        assertThat(client.getJob(67000000), is(Optional.empty()));
        assertThat(client.cancel(67000000), is(false));
        server.verify();
    }

    @Test
    public void noChangeKeepsTheCursor() throws Exception {
        server.expect(requestTo(URL + V + "/jobs?update_time=1794001386")).andRespond(recorded(HttpStatus.OK, "jobs-no-change"));
        final SlurmJobSnapshot snapshot = client.getJobs(1794001386L);
        assertThat(snapshot.jobs().isEmpty(), is(true));
        // The response carries last_update 0, which must not reset the cursor.
        assertThat(snapshot.lastUpdate(), is(1794001386L));
    }
}
