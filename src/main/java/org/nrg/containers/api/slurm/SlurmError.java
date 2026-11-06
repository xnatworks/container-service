package org.nrg.containers.api.slurm;

import com.google.auto.value.AutoValue;

import javax.annotation.Nullable;

/** One entry from the {@code errors} array that slurmrestd includes in every response. */
@AutoValue
public abstract class SlurmError {
    /** {@code SLURM_NO_CHANGE_IN_DATA}: nothing changed since the {@code update_time} the caller sent. */
    public static final int NO_CHANGE_IN_DATA = 1900;
    /** {@code ESLURM_INVALID_JOB_ID}: the controller does not know the job, or has already purged it. */
    public static final int INVALID_JOB_ID = 2017;
    /** {@code ESLURM_ALREADY_DONE}: the job already finished, so there is nothing to signal or cancel. */
    public static final int ALREADY_DONE = 2021;

    @Nullable public abstract Integer errorNumber();
    @Nullable public abstract String error();
    @Nullable public abstract String description();
    @Nullable public abstract String source();

    public static SlurmError create(@Nullable final Integer errorNumber, @Nullable final String error,
                                    @Nullable final String description, @Nullable final String source) {
        return new AutoValue_SlurmError(errorNumber, error, description, source);
    }

    @Override
    public String toString() {
        final StringBuilder text = new StringBuilder();
        if (description() != null) {
            text.append(description());
        }
        if (error() != null && !error().equals(description())) {
            text.append(text.length() > 0 ? " (" : "(").append(error()).append(')');
        }
        if (errorNumber() != null) {
            text.append(text.length() > 0 ? " " : "").append("[error ").append(errorNumber()).append(']');
        }
        if (source() != null) {
            text.append(" from ").append(source());
        }
        return text.toString();
    }
}
