package org.nrg.containers.api.slurm;

import com.google.auto.value.AutoValue;

/** The user name and JWT sent with a request, as {@code X-SLURM-USER-NAME} and {@code X-SLURM-USER-TOKEN}. */
@AutoValue
public abstract class SlurmCredentials {
    public abstract String user();
    public abstract String token();

    public static SlurmCredentials create(final String user, final String token) {
        return new AutoValue_SlurmCredentials(user, token);
    }

    @Override
    public String toString() {
        return "SlurmCredentials{user=" + user() + ", token=****}";
    }
}
