package org.nrg.containers.api.slurm;

import org.junit.Test;
import org.nrg.containers.model.server.docker.SlurmSettings;

import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;

public class SlurmClientFactoryImplTest {
    private static SlurmSettings.Builder settings() {
        return SlurmSettings.builder().restUrl("https://slurm.example.org:6820").user("xnat").token("t1");
    }

    @Test
    public void reusesTheClientWhileTheSettingsAreEqual() {
        final SlurmClientFactoryImpl factory = new SlurmClientFactoryImpl();
        assertThat(factory.getClient(settings().build()), sameInstance(factory.getClient(settings().build())));
    }

    @Test
    public void anyChangedSettingReplacesTheClient() {
        final SlurmClientFactoryImpl factory = new SlurmClientFactoryImpl();
        final SlurmClient first = factory.getClient(settings().build());
        assertThat(factory.getClient(settings().token("t2").build()), not(sameInstance(first)));

        final SlurmClient second = factory.getClient(settings().apiVersion("v0.0.40").build());
        assertThat(second, not(sameInstance(first)));
        assertThat(factory.getClient(settings().apiVersion("v0.0.40").partition("gpu").build()), not(sameInstance(second)));
    }
}
