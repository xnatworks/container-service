package org.nrg.containers.api.slurm;

import com.google.common.collect.ImmutableMap;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.nrg.containers.exceptions.ContainerException;
import org.nrg.containers.model.container.auto.Container;
import org.nrg.containers.model.container.auto.Container.ContainerMount;
import org.nrg.containers.model.server.docker.Backend;
import org.nrg.containers.model.server.docker.SlurmSettings;
import org.nrg.containers.secrets.EnvironmentVariableResolvedSecret;
import org.nrg.containers.secrets.EnvironmentVariableSecretDestination;
import org.nrg.containers.secrets.Secret;
import org.nrg.containers.secrets.SystemPropertySecretSource;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

public class SlurmJobBuilderTest {
    private static final SlurmJobPaths PATHS = SlurmJobPaths.create("/data/xnat/build/slurm/42", "/cluster/build/slurm/42");

    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    private static SlurmSettings.Builder settings() {
        return SlurmSettings.builder().restUrl("https://slurm:6820").user("xnat").token("t").partition("batch").timeLimit("4:00:00");
    }

    private static Container.Builder container() {
        return Container.builder()
                .databaseId(42L)
                .commandId(7L)
                .wrapperId(8L)
                .userId("admin")
                .backend(Backend.SLURM)
                .containerName("dcm2niix-42")
                .dockerImage("xnat/dcm2niix:1.0.2")
                .commandLine("dcm2niix -o /output /input");
    }

    private static ContainerMount mount(final String name, final String hostPath, final String containerPath, final boolean writable) {
        return ContainerMount.builder()
                .databaseId(0L)
                .name(name)
                .writable(writable)
                .xnatHostPath(hostPath)
                .containerHostPath(hostPath)
                .containerPath(containerPath)
                .inputFiles(Collections.emptyList())
                .build();
    }

    @Test
    public void runKeepsTheEntrypointAndQuotesEachArgument() throws Exception {
        final String script = SlurmJobBuilder.build(container().commandLine("tool --name \"two words\" it\\'s").build(),
                settings().build(), PATHS).script();

        assertThat(script, startsWith("#!/bin/bash -l\n"));
        assertThat(script, containsString("apptainer run --no-eval --cleanenv --contain --writable-tmpfs --workdir \"$JOB_DIR/work\""));
        assertThat(script, containsString("\"$IMAGE\" 'tool' '--name' 'two words' 'it'\\''s'\n"));
        assertThat(script, containsString("IMAGE='docker://xnat/dcm2niix:1.0.2'"));
        assertThat(script, containsString("finish $?"));
    }

    @Test
    public void overrideEntrypointRunsTheCommandLineThroughSh() throws Exception {
        final String script = SlurmJobBuilder.build(container().overrideEntrypoint(true)
                .commandLine("dcm2niix -o /output /input > /output/log.txt").build(), settings().build(), PATHS).script();

        assertThat(script, containsString("apptainer exec "));
        assertThat(script, containsString("\"$IMAGE\" /bin/sh -c 'dcm2niix -o /output /input > /output/log.txt'\n"));
    }

    @Test
    public void mountsWorkingDirectoryAndGpusBecomeRuntimeOptions() throws Exception {
        final Container container = container()
                .workingDirectory("/work dir")
                .gpus("2")
                .mounts(Arrays.asList(mount("in", "/cluster/build/abc", "/input", false), mount("out", "/cluster/build/def", "/output", true)))
                .build();

        final SlurmJobRequest request = SlurmJobBuilder.build(container, settings().runtime("singularity").build(), PATHS);

        assertThat(request.script(), containsString("singularity run "));
        assertThat(request.script(), containsString(" --nv --pwd '/work dir'"));
        assertThat(request.script(), containsString("--bind '/cluster/build/abc:/input:ro'"));
        assertThat(request.script(), containsString("--bind '/cluster/build/def:/output'"));
        assertThat(request.tresPerNode(), is("gres/gpu:2"));
    }

    @Test
    public void requestUsesTheJobDirectoryAndServerWideSettings() throws Exception {
        final SlurmJobRequest request = SlurmJobBuilder.build(container().limitCpu(1.5).limitMemory(8192L).build(), settings().build(), PATHS);

        assertThat(request.name(), is("xnat-dcm2niix-42"));
        assertThat(request.workingDirectory(), is("/cluster/build/slurm/42"));
        assertThat(request.standardOutput(), is("/cluster/build/slurm/42/stdout.log"));
        assertThat(request.standardError(), is("/cluster/build/slurm/42/stderr.log"));
        assertThat(request.partition(), is("batch"));
        assertThat(request.timeLimitMinutes(), is(240));
        assertThat(request.cpusPerTask(), is(2));
        assertThat(request.memoryPerNodeMb(), is(8192L));
        assertThat(request.tresPerNode(), is(nullValue()));
        assertThat(request.hold(), is(true));
        assertThat(request.comment(), is("XNAT container 42 launched by admin"));
        assertThat(request.script(), containsString("JOB_DIR='/cluster/build/slurm/42'"));
    }

    @Test
    public void reservedMemoryIsUsedWhenThereIsNoLimit() throws Exception {
        assertThat(SlurmJobBuilder.build(container().reserveMemory(2048L).build(), settings().build(), PATHS).memoryPerNodeMb(), is(2048L));
    }

    @Test
    public void environmentAndSecretsGoIntoTheJobEnvironmentNotTheScript() throws Exception {
        final String property = "slurm.test.secret." + System.nanoTime();
        final Secret secret = new Secret(new SystemPropertySecretSource(property), new EnvironmentVariableSecretDestination("API_KEY"));
        final Container container = container()
                .environmentVariables(ImmutableMap.of("XNAT_HOST", "https://xnat.example.org"))
                .secrets(Collections.singletonList(new EnvironmentVariableResolvedSecret(secret, "s3cr3t")))
                .build();

        final SlurmJobRequest request = SlurmJobBuilder.build(container, settings().build(), PATHS);

        assertThat(request.environment(), contains(SlurmJobRequest.DEFAULT_PATH, "APPTAINERENV_API_KEY=s3cr3t",
                "APPTAINERENV_XNAT_HOST=https://xnat.example.org"));
        assertThat(request.script(), not(containsString("s3cr3t")));
        assertThat(request.toString(), not(containsString("s3cr3t")));
    }

    @Test
    public void singularityUsesItsOwnEnvironmentPrefix() throws Exception {
        final SlurmJobRequest request = SlurmJobBuilder.build(container().environmentVariables(ImmutableMap.of("A", "1")).build(),
                settings().runtime("singularity").pullCacheDirectory("/shared/cache").build(), PATHS);
        assertThat(request.environment(), hasItem("SINGULARITYENV_A=1"));
        assertThat(request.script(), containsString("export SINGULARITY_CACHEDIR='/shared/cache'"));
    }

    @Test
    public void aSifIsPreferredAndRegistryPullsAreTheFallback() throws Exception {
        final String script = SlurmJobBuilder.build(container().build(),
                settings().sifDirectory("/shared/sif/").pullCacheDirectory("/shared/cache").build(), PATHS).script();

        assertThat(script, containsString("export APPTAINER_CACHEDIR='/shared/cache'"));
        assertThat(script, containsString("SIF='/shared/sif/xnat_dcm2niix_1.0.2.sif'"));
        assertThat(script, containsString("if [ -f \"$SIF\" ]; then\n    IMAGE=\"$SIF\"\nelse\n    IMAGE='docker://xnat/dcm2niix:1.0.2'\nfi"));
    }

    @Test
    public void withoutRegistryPullsAMissingSifFailsTheJob() throws Exception {
        final String script = SlurmJobBuilder.build(container().build(),
                settings().sifDirectory("/shared/sif").allowDockerPull(false).pullCacheDirectory("/shared/cache").build(), PATHS).script();

        assertThat(script, not(containsString("docker://")));
        assertThat(script, not(containsString("CACHEDIR")));
        assertThat(script, containsString("registry pulls are disabled"));
        assertThat(script, containsString("finish " + SlurmJobBuilder.EXIT_IMAGE_UNAVAILABLE));
    }

    @Test
    public void sifFileNamesAreDeterministic() {
        assertThat(SlurmJobBuilder.sifFileName("xnat/dcm2niix:1.0.2"), is("xnat_dcm2niix_1.0.2.sif"));
        assertThat(SlurmJobBuilder.sifFileName("ubuntu"), is("ubuntu_latest.sif"));
        assertThat(SlurmJobBuilder.sifFileName("docker.io/library/ubuntu:22.04"), is("ubuntu_22.04.sif"));
        assertThat(SlurmJobBuilder.sifFileName("registry.example.org:5000/lab/tool"), is("registry.example.org_5000_lab_tool_latest.sif"));
        assertThat(SlurmJobBuilder.sifFileName("lab/tool@sha256:abc123"), is("lab_tool_sha256_abc123.sif"));
    }

    @Test
    public void gpuRequestsComeFromGpusOrGenericResources() {
        assertThat(SlurmJobBuilder.gpuCount(container().gpus("all").build()), is(1));
        assertThat(SlurmJobBuilder.gpuCount(container().gpus("0").build()), is(nullValue()));
        assertThat(SlurmJobBuilder.gpuCount(container().genericResources(ImmutableMap.of("NVIDIA-GPU", "3")).build()), is(3));
        assertThat(SlurmJobBuilder.gpuCount(container().genericResources(ImmutableMap.of("fpga", "1")).build()), is(nullValue()));
        assertThat(SlurmJobBuilder.gpuCount(container().build()), is(nullValue()));
    }

    @Test
    public void pathsApptainerCannotBindAreRejected() {
        try {
            SlurmJobBuilder.build(container().mounts(Collections.singletonList(mount("in", "/data/a:b", "/input", false))).build(),
                    settings().build(), PATHS);
            fail("Expected the mount to be rejected");
        } catch (ContainerException e) {
            assertThat(e.getMessage(), containsString("\"in\""));
        }
    }

    @Test
    public void invalidEnvironmentVariableNamesAreRejected() {
        try {
            SlurmJobBuilder.build(container().environmentVariables(ImmutableMap.of("BAD-NAME", "x")).build(), settings().build(), PATHS);
            fail("Expected the variable to be rejected");
        } catch (ContainerException e) {
            assertThat(e.getMessage(), containsString("BAD-NAME"));
        }
    }

    @Test
    public void jobPathsAreTranslatedLikeMounts() {
        final SlurmJobPaths paths = SlurmJobPaths.translate("/data/xnat/build/slurm/42/", "/data/xnat/build", "/cluster/build/");
        assertThat(paths.xnatPath(), is("/data/xnat/build/slurm/42"));
        assertThat(paths.clusterPath(), is("/cluster/build/slurm/42"));
        assertThat(paths.xnatFile(SlurmJobPaths.EXIT_CODE), is("/data/xnat/build/slurm/42/exit-code"));

        assertThat(SlurmJobPaths.translate("/data/xnat/build2/x", "/data/xnat/build", "/cluster").clusterPath(), is("/data/xnat/build2/x"));
        assertThat(SlurmJobPaths.translate("/data/x", null, null).clusterPath(), is("/data/x"));
    }

    /** Runs the generated script with a stand-in apptainer that records its arguments, to prove the quoting. */
    @Test
    public void theScriptRunsAndRecordsTheExitCode() throws Exception {
        assumeTrue("bash is needed", new File("/bin/bash").canExecute());
        final Path jobDir = temp.newFolder("job").toPath();
        final Path bin = temp.newFolder("bin").toPath();
        final Path argsFile = jobDir.resolve("args");
        final Path fake = bin.resolve("apptainer");
        Files.write(fake, ("#!/bin/bash\nprintf '%s\\n' \"$@\" > '" + argsFile + "'\nexit 3\n").getBytes(StandardCharsets.UTF_8));
        assertThat(fake.toFile().setExecutable(true), is(true));

        final SlurmJobPaths paths = SlurmJobPaths.create(jobDir.toString(), jobDir.toString());
        final Container container = container()
                .commandLine("tool --label \"two words\" 'single $HOME' it\\'s")
                .mounts(Collections.singletonList(mount("in", "/data/in put", "/input", false)))
                .build();
        final Path script = jobDir.resolve("job.sh");
        Files.write(script, SlurmJobBuilder.build(container, settings().build(), paths).script().getBytes(StandardCharsets.UTF_8));

        final ProcessBuilder process = new ProcessBuilder("/bin/bash", script.toString()).redirectErrorStream(true);
        process.environment().put("PATH", bin + ":/usr/bin:/bin");
        final Process run = process.start();
        assertThat(run.waitFor(30, TimeUnit.SECONDS), is(true));

        assertThat(run.exitValue(), is(3));
        assertThat(new String(Files.readAllBytes(jobDir.resolve(SlurmJobPaths.EXIT_CODE)), StandardCharsets.UTF_8).trim(), is("3"));
        assertThat(Files.isDirectory(jobDir.resolve(SlurmJobPaths.WORK)), is(true));
        final List<String> args = Files.readAllLines(argsFile, StandardCharsets.UTF_8);
        assertThat(args, contains("run", "--no-eval", "--cleanenv", "--contain", "--writable-tmpfs", "--workdir", jobDir.resolve("work").toString(),
                "--bind", "/data/in put:/input:ro", "docker://xnat/dcm2niix:1.0.2",
                "tool", "--label", "two words", "single $HOME", "it's"));
    }
}
