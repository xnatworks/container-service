package org.nrg.containers.api.slurm;

import org.nrg.containers.exceptions.ContainerBackendException;

import java.util.Collections;
import java.util.List;

/** slurmrestd answered, but with an HTTP error status or with entries in its {@code errors} array. */
public class SlurmApiException extends ContainerBackendException {
    private final int httpStatus;
    private final List<SlurmError> errors;

    public SlurmApiException(final String message, final int httpStatus, final List<SlurmError> errors) {
        super(message);
        this.httpStatus = httpStatus;
        this.errors = Collections.unmodifiableList(errors);
    }

    public int getHttpStatus() {
        return httpStatus;
    }

    public List<SlurmError> getErrors() {
        return errors;
    }

    public boolean hasError(final int errorNumber) {
        return errors.stream().anyMatch(e -> e.errorNumber() != null && e.errorNumber() == errorNumber);
    }

    public boolean isAuthenticationFailure() {
        return httpStatus == 401 || httpStatus == 403;
    }
}
