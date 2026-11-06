package org.nrg.containers.api;

import com.google.auto.value.AutoValue;

/**
 * Part of a log read from a byte offset, for backends whose logs are files without per-line timestamps (Slurm).
 * Pass {@link #nextOffset()} to the next read to get only what was written since.
 */
@AutoValue
public abstract class LogChunk {
    public abstract String content();
    public abstract long nextOffset();

    public static LogChunk create(final String content, final long nextOffset) {
        return new AutoValue_LogChunk(content, nextOffset);
    }
}
