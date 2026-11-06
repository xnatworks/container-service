package org.nrg.containers.model.server.docker;

import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public enum Backend {
    DOCKER,
    SWARM,
    KUBERNETES,
    SLURM;

    @JsonValue
    @Override
    public String toString() {
        return this.name().toLowerCase();
    }

    // SLURM is left out until swarm-style constraints are mapped to Slurm node features (--constraint).
    public static final Set<Backend> SUPPORTS_CONSTRAINTS =
            Stream.of(Backend.SWARM, Backend.KUBERNETES).collect(Collectors.toSet());
}
