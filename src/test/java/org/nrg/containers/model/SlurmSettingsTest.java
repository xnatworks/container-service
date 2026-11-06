package org.nrg.containers.model;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import org.junit.Test;
import org.nrg.containers.exceptions.InvalidDefinitionException;
import org.nrg.containers.model.server.docker.Backend;
import org.nrg.containers.model.server.docker.DockerServerBase.DockerServer;
import org.nrg.containers.model.server.docker.DockerServerBase.DockerServerWithPing;
import org.nrg.containers.model.server.docker.DockerServerEntity;
import org.nrg.containers.model.server.docker.SlurmSettings;

import java.util.Date;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class SlurmSettingsTest {
    private static final String TOKEN = "eyJhbGciOi.secret.token";

    private final ObjectMapper mapper = new ObjectMapper().registerModule(new GuavaModule());

    private static SlurmSettings.Builder valid() {
        return SlurmSettings.builder().restUrl("https://slurm.example.org:6820").user("xnat").token(TOKEN);
    }

    private static DockerServer slurmServer(final SlurmSettings slurm) {
        return DockerServer.builder().name("hpc").backend(Backend.SLURM).slurm(slurm).build();
    }

    @Test
    public void timeLimitAcceptsEverySbatchFormat() {
        assertThat(SlurmSettings.parseTimeLimitMinutes("90"), is(90));
        assertThat(SlurmSettings.parseTimeLimitMinutes("90:30"), is(91));        // leftover seconds round up
        assertThat(SlurmSettings.parseTimeLimitMinutes("4:00:00"), is(240));
        assertThat(SlurmSettings.parseTimeLimitMinutes("1:30:01"), is(91));
        assertThat(SlurmSettings.parseTimeLimitMinutes("2-0"), is(2880));
        assertThat(SlurmSettings.parseTimeLimitMinutes("1-12"), is(2160));
        assertThat(SlurmSettings.parseTimeLimitMinutes("1-12:30"), is(2190));
        assertThat(SlurmSettings.parseTimeLimitMinutes("1-12:30:15"), is(2191));
        assertThat(SlurmSettings.parseTimeLimitMinutes(" 45 "), is(45));
    }

    @Test
    public void unlimitedAndBlankTimeLimitsMeanNoLimit() {
        assertThat(SlurmSettings.parseTimeLimitMinutes(null), is(nullValue()));
        assertThat(SlurmSettings.parseTimeLimitMinutes(""), is(nullValue()));
        assertThat(SlurmSettings.parseTimeLimitMinutes("UNLIMITED"), is(nullValue()));
        assertThat(SlurmSettings.parseTimeLimitMinutes("infinite"), is(nullValue()));
    }

    @Test
    public void malformedTimeLimitsAreRejected() {
        for (final String bad : new String[]{"abc", "1:2:3:4", "-5", "1-", "0", "0:00:00", "1.5"}) {
            try {
                SlurmSettings.parseTimeLimitMinutes(bad);
                fail("Expected \"" + bad + "\" to be rejected");
            } catch (IllegalArgumentException e) {
                assertThat(e.getMessage(), containsString(bad));
            }
        }
    }

    @Test
    public void validSettingsPassValidation() throws Exception {
        slurmServer(valid().timeLimit("1-00:00:00").build()).validate();
    }

    @Test
    public void slurmBackendDoesNotNeedADockerHost() throws Exception {
        // Docker and Swarm require a host; Slurm is reached through its REST URL instead.
        slurmServer(valid().build()).validate();
    }

    @Test
    public void slurmBackendRequiresSlurmSettings() {
        assertInvalid(slurmServer(null), "Slurm settings are required");
    }

    @Test
    public void requiredFieldsAreReported() {
        assertInvalid(slurmServer(SlurmSettings.builder().build()),
                "REST URL cannot be empty", "user cannot be empty", "token cannot be empty");
    }

    @Test
    public void badValuesAreReported() {
        assertInvalid(slurmServer(valid().restUrl("slurm.example.org").build()), "http or https URL");
        assertInvalid(slurmServer(valid().apiVersion("44").build()), "API version");
        assertInvalid(slurmServer(valid().runtime("docker").build()), "runtime");
        assertInvalid(slurmServer(valid().timeLimit("forever").build()), "time limit");
    }

    @Test
    public void disablingRegistryPullsRequiresASifDirectory() throws Exception {
        assertInvalid(slurmServer(valid().allowDockerPull(false).build()), "SIF directory is required");
        slurmServer(valid().allowDockerPull(false).sifDirectory("/shared/sif").build()).validate();
    }

    @Test
    public void jsonDefaultsApply() throws Exception {
        final DockerServer server = mapper.readValue("{\"name\":\"hpc\",\"backend\":\"slurm\","
                + "\"slurm\":{\"rest-url\":\"https://slurm:6820\",\"user\":\"xnat\",\"token\":\"t\",\"runtime\":\" Singularity \"}}",
                DockerServer.class);
        final SlurmSettings slurm = server.slurm();
        assertThat(slurm.apiVersion(), is(SlurmSettings.DEFAULT_API_VERSION));
        assertThat(slurm.runtime(), is("singularity"));
        assertThat(slurm.allowDockerPull(), is(true));
        assertThat(slurm.token(), is("t"));
    }

    /** The token goes in on POST but never comes back out of GET /xapi/docker/server. */
    @Test
    public void tokenIsWriteOnly() throws Exception {
        final DockerServerWithPing withPing = DockerServerWithPing.create(slurmServer(valid().build()), true);
        final JsonNode json = mapper.valueToTree(withPing);

        assertThat(json.get("slurm").has("token"), is(false));
        assertThat(json.get("slurm").get("token-set").asBoolean(), is(true));
        assertThat(json.get("slurm").get("user").asText(), is("xnat"));
        assertThat(mapper.writeValueAsString(withPing), not(containsString(TOKEN)));
    }

    @Test
    public void toStringMasksTheToken() {
        assertThat(slurmServer(valid().build()).toString(), not(containsString(TOKEN)));
    }

    @Test
    public void entityRoundTripKeepsEverySetting() {
        final SlurmSettings slurm = valid().partition("gpu").account("lab").qos("normal").timeLimit("2:00:00")
                .runtime("singularity").sifDirectory("/shared/sif").allowDockerPull(false)
                .pullCacheDirectory("/shared/cache").build();

        final DockerServer roundTripped = DockerServer.create(DockerServerEntity.create(slurmServer(slurm)));

        assertThat(roundTripped.backend(), is(Backend.SLURM));
        assertThat(roundTripped.slurm(), is(slurm));
    }

    @Test
    public void nonSlurmServersHaveNoSlurmSettings() {
        final DockerServer docker = DockerServer.builder().name("d").host("unix:///var/run/docker.sock").build();
        assertThat(DockerServer.create(DockerServerEntity.create(docker)).slurm(), is(nullValue()));
    }

    /** Runs on the status polling path, so dropping the Slurm settings would wipe them every few seconds. */
    @Test
    public void updateEventCheckTimePreservesSlurmSettings() {
        final DockerServer server = slurmServer(valid().partition("gpu").build());
        assertThat(server.updateEventCheckTime(new Date()).slurm(), is(server.slurm()));
    }

    private static void assertInvalid(final DockerServer server, final String... messages) {
        try {
            server.validate();
            fail("Expected validation to fail");
        } catch (InvalidDefinitionException e) {
            for (final String message : messages) {
                assertThat(e.getMessage(), containsString(message));
            }
        }
    }
}
