package org.nrg.containers.services;

import org.junit.Before;
import org.junit.Test;
import org.nrg.containers.api.KubernetesClientFactory;
import org.nrg.containers.exceptions.InvalidDefinitionException;
import org.nrg.containers.model.server.docker.Backend;
import org.nrg.containers.model.server.docker.DockerServerBase.DockerServer;
import org.nrg.containers.model.server.docker.DockerServerEntity;
import org.nrg.containers.model.server.docker.SlurmSettings;
import org.nrg.containers.services.impl.DockerServerServiceImpl;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * GET /xapi/docker/server never returns the Slurm token, so saving the configuration back must keep the stored
 * token, but only while the REST URL is unchanged.
 */
public class SlurmTokenCarryOverTest {
    private static final String URL = "https://slurm.example.org:6820";
    private static final String STORED_TOKEN = "stored-token";

    private DockerServerEntityService entityService;
    private DockerServerServiceImpl service;

    @Before
    public void setUp() {
        entityService = mock(DockerServerEntityService.class);
        service = new DockerServerServiceImpl(entityService, mock(KubernetesClientFactory.class));
    }

    private static DockerServer server(final long id, final String url, final String token) {
        return DockerServer.builder().id(id).name("hpc").backend(Backend.SLURM)
                .slurm(SlurmSettings.builder().restUrl(url).user("xnat").token(token).build())
                .build();
    }

    private static DockerServerEntity storedEntity() {
        return DockerServerEntity.create(server(0L, URL, STORED_TOKEN));
    }

    @Test
    public void updatingAServerWithoutATokenKeepsTheStoredOne() throws Exception {
        final DockerServerEntity stored = storedEntity();
        when(entityService.retrieve(7L)).thenReturn(stored);

        final DockerServerEntity saved = service.fromPojo(server(7L, URL, null));

        assertThat(saved.getSlurmToken(), is(STORED_TOKEN));
    }

    @Test
    public void savingANewRowWithoutATokenKeepsTheEnabledServersToken() throws Exception {
        // The admin UI saves by creating a new row, which disables the old one.
        when(entityService.retrieve(anyLong())).thenReturn(null);
        when(entityService.getServer()).thenReturn(storedEntity());

        final DockerServerEntity saved = service.fromPojo(server(0L, URL, null));

        assertThat(saved.getSlurmToken(), is(STORED_TOKEN));
    }

    /** The admin UI submits its (hidden) Slurm fields whatever the backend; a Docker server must not keep them. */
    @Test
    public void otherBackendsDropSlurmSettings() throws Exception {
        final DockerServer docker = DockerServer.builder().name("d").backend(Backend.DOCKER).host("unix:///var/run/docker.sock")
                .slurm(SlurmSettings.builder().build()).build();

        final DockerServerEntity saved = service.fromPojo(docker);

        assertThat(saved.getSlurmToken(), is(org.hamcrest.Matchers.nullValue()));
        assertThat(saved.getSlurmRuntime(), is(org.hamcrest.Matchers.nullValue()));
    }

    @Test
    public void aNewTokenReplacesTheStoredOne() throws Exception {
        when(entityService.retrieve(7L)).thenReturn(storedEntity());

        assertThat(service.fromPojo(server(7L, URL, "new-token")).getSlurmToken(), is("new-token"));
    }

    @Test
    public void changingTheUrlWithoutATokenIsRejected() {
        when(entityService.retrieve(7L)).thenReturn(storedEntity());

        try {
            service.fromPojo(server(7L, "https://attacker.example.com", null));
            fail("The stored token must not be sent to a new URL");
        } catch (InvalidDefinitionException e) {
            assertThat(e.getMessage(), containsString("token cannot be empty"));
        }
    }
}
