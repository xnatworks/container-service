package org.nrg.containers.model.server.docker;

import org.nrg.containers.model.server.docker.DockerServerBase.DockerServer;
import org.nrg.framework.orm.hibernate.AbstractHibernateEntity;

import javax.persistence.*;
import java.util.*;

@Entity
public class DockerServerEntity extends AbstractHibernateEntity {
    private String name;
    private String host;
    private String certPath;
    private Date lastEventCheckTime;
    private boolean swarmMode;
    private Backend backend;
    private String pathTranslationXnatPrefix;
    private String pathTranslationDockerPrefix;
    private Boolean pullImagesOnXnatInit;
    private String containerUser;
    private List<DockerServerEntitySwarmConstraint> swarmConstraints = new ArrayList<>();
    private List<DockerServerEntityKubernetesToleration> kubernetesTolerations = new ArrayList<>();
    private boolean autoCleanup = true;
    private Integer maxConcurrentFinalizingJobs;
    private boolean statusEmailEnabled = true;
    private boolean buildDirCleanupEnabled = false;
    private Integer buildDirRetainDaysCompleted = DockerServerBase.DEFAULT_BUILD_DIR_RETAIN_DAYS_COMPLETED;
    private Integer buildDirRetainDaysFailed = DockerServerBase.DEFAULT_BUILD_DIR_RETAIN_DAYS_FAILED;
    private Integer buildDirRetainDaysKilled = DockerServerBase.DEFAULT_BUILD_DIR_RETAIN_DAYS_KILLED;
    private String buildDirCleanupTime = DockerServerBase.DEFAULT_BUILD_DIR_CLEANUP_TIME;
    private String gpuVendor;
    private String archivePvcName;
    private String buildPvcName;
    private String combinedPvcName;
    private String archivePathTranslation;
    private String buildPathTranslation;
    private String combinedPathTranslation;
    private String slurmRestUrl;
    private String slurmApiVersion;
    private String slurmUser;
    private String slurmToken;
    private String slurmPartition;
    private String slurmAccount;
    private String slurmQos;
    private String slurmTimeLimit;
    private String slurmRuntime;
    private String slurmSifDirectory;
    private Boolean slurmAllowDockerPull;
    private String slurmPullCacheDirectory;

    public static DockerServerEntity create(final DockerServer dockerServer) {
        return new DockerServerEntity().update(dockerServer);
    }

    public DockerServerEntity update(final DockerServer dockerServer) {
        this.host = dockerServer.host();
        this.name = dockerServer.name();
        this.certPath = dockerServer.certPath();
        this.backend = dockerServer.backend();
        this.lastEventCheckTime = dockerServer.lastEventCheckTime();
        this.pathTranslationXnatPrefix = dockerServer.pathTranslationXnatPrefix();
        this.pathTranslationDockerPrefix = dockerServer.pathTranslationDockerPrefix();
        this.pullImagesOnXnatInit = dockerServer.pullImagesOnXnatInit();
        this.containerUser = dockerServer.containerUser();
        this.autoCleanup = dockerServer.autoCleanup();
        this.maxConcurrentFinalizingJobs = dockerServer.maxConcurrentFinalizingJobs();
        this.statusEmailEnabled = dockerServer.statusEmailEnabled();
        this.buildDirCleanupEnabled = dockerServer.buildDirCleanupEnabled();
        this.buildDirRetainDaysCompleted = dockerServer.buildDirRetainDaysCompleted();
        this.buildDirRetainDaysFailed = dockerServer.buildDirRetainDaysFailed();
        this.buildDirRetainDaysKilled = dockerServer.buildDirRetainDaysKilled();
        this.buildDirCleanupTime = dockerServer.buildDirCleanupTime();
        this.gpuVendor = dockerServer.gpuVendor();
        this.archivePvcName = dockerServer.archivePvcName();
        this.buildPvcName = dockerServer.buildPvcName();
        this.combinedPvcName = dockerServer.combinedPvcName();
        this.archivePathTranslation = dockerServer.archivePathTranslation();
        this.buildPathTranslation = dockerServer.buildPathTranslation();
        this.combinedPathTranslation = dockerServer.combinedPathTranslation();
        updateSlurm(dockerServer.slurm());

        final Map<String, DockerServerBase.DockerServerSwarmConstraint> pojoConstraintsToAdd = new HashMap<>();
        List<DockerServerBase.DockerServerSwarmConstraint> pojoConstraints = dockerServer.swarmConstraints();
        pojoConstraints = pojoConstraints == null ? Collections.emptyList()
                : pojoConstraints;
        for (final DockerServerBase.DockerServerSwarmConstraint constraint : pojoConstraints) {
            pojoConstraintsToAdd.put(constraint.attribute(), constraint);
        }
        final List<DockerServerEntitySwarmConstraint> toRemove = new ArrayList<>();
        for (final DockerServerEntitySwarmConstraint constraintEntity : this.swarmConstraints) {
            String attr = constraintEntity.getAttribute();
            if (pojoConstraintsToAdd.containsKey(attr)) {
                // Already have the constraint, update it here and remove it from toAdd
                constraintEntity.update(pojoConstraintsToAdd.get(attr));
                pojoConstraintsToAdd.remove(attr);
            } else {
                // Constraint was removed
                toRemove.add(constraintEntity);
            }
        }
        for (final DockerServerBase.DockerServerSwarmConstraint constraint : pojoConstraintsToAdd.values()) {
            this.addSwarmConstraint(DockerServerEntitySwarmConstraint.fromPojo(constraint));
        }
        for (final DockerServerEntitySwarmConstraint constraintEntity : toRemove) {
            this.removeSwarmConstraint(constraintEntity);
        }

        final Map<String, DockerServerBase.KubernetesToleration> pojoTolerationsToAdd = new HashMap<>();
        List<DockerServerBase.KubernetesToleration> pojoTolerations = dockerServer.kubernetesTolerations();
        pojoTolerations = pojoTolerations == null ? Collections.emptyList() : pojoTolerations;
        for (final DockerServerBase.KubernetesToleration toleration : pojoTolerations) {
            pojoTolerationsToAdd.put(tolerationCompositeKey(toleration.key(), toleration.operator(), toleration.effect()), toleration);
        }
        final List<DockerServerEntityKubernetesToleration> tolerationsToRemove = new ArrayList<>();
        for (final DockerServerEntityKubernetesToleration tolerationEntity : this.kubernetesTolerations) {
            String key = tolerationCompositeKey(tolerationEntity.getTolerationKey(), tolerationEntity.getOperator(), tolerationEntity.getEffect());
            if (pojoTolerationsToAdd.containsKey(key)) {
                tolerationEntity.update(pojoTolerationsToAdd.get(key));
                pojoTolerationsToAdd.remove(key);
            } else {
                tolerationsToRemove.add(tolerationEntity);
            }
        }
        for (final DockerServerBase.KubernetesToleration toleration : pojoTolerationsToAdd.values()) {
            this.addKubernetesToleration(DockerServerEntityKubernetesToleration.fromPojo(toleration));
        }
        for (final DockerServerEntityKubernetesToleration tolerationEntity : tolerationsToRemove) {
            this.removeKubernetesToleration(tolerationEntity);
        }

        return this;
    }

    public String getName() {
        return name;
    }

    public void setName(final String name) {
        this.name = name;
    }

    public String getHost() {
        return host;
    }

    public void setHost(final String host) {
        this.host = host;
    }

    public String getCertPath() {
        return certPath;
    }

    public void setCertPath(final String certPath) {
        this.certPath = certPath;
    }

    public Date getLastEventCheckTime() {
        return lastEventCheckTime;
    }

    public void setLastEventCheckTime(final Date lastEventCheckTime) {
        this.lastEventCheckTime = lastEventCheckTime;
    }

    @Deprecated
    public boolean getSwarmMode() {
        return getBackend() == Backend.SWARM;
    }

    @Deprecated
    public void setSwarmMode(final boolean swarmMode) {
        this.swarmMode = swarmMode;
    }

    @Enumerated(EnumType.STRING)
    public Backend getBackend() {
        return backend;
    }

    public void setBackend(Backend backend) {
        if (backend == null) {
            // Default for backwards compatibility
            backend = swarmMode ? Backend.SWARM : Backend.DOCKER;
        }
        this.backend = backend;
    }

    public String getPathTranslationXnatPrefix() {
        return pathTranslationXnatPrefix;
    }

    public void setPathTranslationXnatPrefix(final String pathTranslationXnatPrefix) {
        this.pathTranslationXnatPrefix = pathTranslationXnatPrefix;
    }

    public String getPathTranslationDockerPrefix() {
        return pathTranslationDockerPrefix;
    }

    public void setPathTranslationDockerPrefix(final String pathTranslationDockerPrefix) {
        this.pathTranslationDockerPrefix = pathTranslationDockerPrefix;
    }

    public Boolean getPullImagesOnXnatInit() {
        return pullImagesOnXnatInit;
    }

    public void setPullImagesOnXnatInit(final Boolean pullImagesOnXnatInit) {
        this.pullImagesOnXnatInit = pullImagesOnXnatInit;
    }

    public String getContainerUser() {
        return containerUser;
    }

    public void setContainerUser(final String containerUser) {
        this.containerUser = containerUser;
    }

    @OneToMany(mappedBy = "dockerServerEntity", cascade = CascadeType.ALL, orphanRemoval = true)
    public List<DockerServerEntitySwarmConstraint> getSwarmConstraints() {
        return swarmConstraints;
    }

    public void setSwarmConstraints(final List<DockerServerEntitySwarmConstraint> swarmConstraints) {
        this.swarmConstraints = swarmConstraints == null ?
                Collections.emptyList() : swarmConstraints;
        for (final DockerServerEntitySwarmConstraint constraint : this.swarmConstraints) {
            constraint.setDockerServerEntity(this);
        }
    }

    public void addSwarmConstraint(final DockerServerEntitySwarmConstraint swarmConstraint) {
        if (swarmConstraint == null || this.swarmConstraints.contains(swarmConstraint)) {
            return;
        }
        this.swarmConstraints.add(swarmConstraint);
        swarmConstraint.setDockerServerEntity(this);
    }

    public void removeSwarmConstraint(final DockerServerEntitySwarmConstraint swarmConstraint) {
        if (swarmConstraint == null || !this.swarmConstraints.contains(swarmConstraint)) {
            return;
        }
        this.swarmConstraints.remove(swarmConstraint);
        swarmConstraint.setDockerServerEntity(null);
    }

    @OneToMany(mappedBy = "dockerServerEntity", cascade = CascadeType.ALL, orphanRemoval = true)
    public List<DockerServerEntityKubernetesToleration> getKubernetesTolerations() {
        return kubernetesTolerations;
    }

    public void setKubernetesTolerations(final List<DockerServerEntityKubernetesToleration> kubernetesTolerations) {
        this.kubernetesTolerations = kubernetesTolerations == null ?
                Collections.emptyList() : kubernetesTolerations;
        for (final DockerServerEntityKubernetesToleration toleration : this.kubernetesTolerations) {
            toleration.setDockerServerEntity(this);
        }
    }

    public void addKubernetesToleration(final DockerServerEntityKubernetesToleration toleration) {
        if (toleration == null || this.kubernetesTolerations.contains(toleration)) {
            return;
        }
        this.kubernetesTolerations.add(toleration);
        toleration.setDockerServerEntity(this);
    }

    public void removeKubernetesToleration(final DockerServerEntityKubernetesToleration toleration) {
        if (toleration == null || !this.kubernetesTolerations.contains(toleration)) {
            return;
        }
        this.kubernetesTolerations.remove(toleration);
        toleration.setDockerServerEntity(null);
    }

    private static String tolerationCompositeKey(String key, String operator, String effect) {
        return (key == null ? "" : key) + "|" + (operator == null ? "" : operator) + "|" + (effect == null ? "" : effect);
    }

    @Column(columnDefinition = "boolean default true")
    public boolean isAutoCleanup() {
        return autoCleanup;
    }

    public void setAutoCleanup(Boolean autoCleanup) {
        this.autoCleanup = autoCleanup == null || autoCleanup;
    }

    public Integer getMaxConcurrentFinalizingJobs() {
        return maxConcurrentFinalizingJobs;
    }

    public void setMaxConcurrentFinalizingJobs(Integer maxConcurrentFinalizingJobs) {
        this.maxConcurrentFinalizingJobs = maxConcurrentFinalizingJobs;
    }

    @Column(columnDefinition = "boolean default true")
    public boolean isStatusEmailEnabled() {
        return statusEmailEnabled;
    }

    public void setStatusEmailEnabled(Boolean statusEmail) {
        this.statusEmailEnabled = statusEmail == null || statusEmail;
    }

    // columnDefinition defaults backfill the existing server row when hbm2ddl adds these columns on upgrade.
    // Fields stay boxed so a stray NULL cannot break hydration; setters coerce to the default.
    @Column(columnDefinition = "boolean default false")
    public boolean isBuildDirCleanupEnabled() {
        return buildDirCleanupEnabled;
    }

    public void setBuildDirCleanupEnabled(Boolean buildDirCleanupEnabled) {
        this.buildDirCleanupEnabled = buildDirCleanupEnabled != null && buildDirCleanupEnabled;
    }

    /**
     * Clamped on the way in, not only validated. A value below the minimum reaching the entity from the
     * database would otherwise fail validate() on every save, including the whole-row rewrite the
     * container status poll performs every ten seconds, which would stop lastEventCheckTime persisting.
     */
    @Column(columnDefinition = "integer default 7")
    public Integer getBuildDirRetainDaysCompleted() {
        return buildDirRetainDaysCompleted;
    }

    public void setBuildDirRetainDaysCompleted(Integer buildDirRetainDaysCompleted) {
        this.buildDirRetainDaysCompleted = buildDirRetainDaysCompleted == null
                ? DockerServerBase.DEFAULT_BUILD_DIR_RETAIN_DAYS_COMPLETED
                : Math.max(DockerServerBase.MIN_BUILD_DIR_RETAIN_DAYS, buildDirRetainDaysCompleted);
    }

    @Column(columnDefinition = "integer default 14")
    public Integer getBuildDirRetainDaysFailed() {
        return buildDirRetainDaysFailed;
    }

    public void setBuildDirRetainDaysFailed(Integer buildDirRetainDaysFailed) {
        this.buildDirRetainDaysFailed = buildDirRetainDaysFailed == null
                ? DockerServerBase.DEFAULT_BUILD_DIR_RETAIN_DAYS_FAILED
                : Math.max(DockerServerBase.MIN_BUILD_DIR_RETAIN_DAYS, buildDirRetainDaysFailed);
    }

    @Column(columnDefinition = "integer default 1")
    public Integer getBuildDirRetainDaysKilled() {
        return buildDirRetainDaysKilled;
    }

    public void setBuildDirRetainDaysKilled(Integer buildDirRetainDaysKilled) {
        this.buildDirRetainDaysKilled = buildDirRetainDaysKilled == null
                ? DockerServerBase.DEFAULT_BUILD_DIR_RETAIN_DAYS_KILLED
                : Math.max(DockerServerBase.MIN_BUILD_DIR_RETAIN_DAYS, buildDirRetainDaysKilled);
    }

    @Column(columnDefinition = "varchar(5) default '02:00'")
    public String getBuildDirCleanupTime() {
        return buildDirCleanupTime;
    }

    public void setBuildDirCleanupTime(String buildDirCleanupTime) {
        this.buildDirCleanupTime = DockerServerBase.isValidCleanupTime(buildDirCleanupTime)
                ? buildDirCleanupTime.trim() : DockerServerBase.DEFAULT_BUILD_DIR_CLEANUP_TIME;
    }

    public String getGpuVendor() {
        return gpuVendor;
    }

    public void setGpuVendor(String gpuVendor) {
        this.gpuVendor = gpuVendor;
    }

    public String getArchivePvcName() { return archivePvcName; }

    public void setArchivePvcName(String archivePvcName) { this.archivePvcName = archivePvcName; }

    public String getBuildPvcName() { return buildPvcName; }

    public void setBuildPvcName(String buildPvcName) { this.buildPvcName = buildPvcName; }

    public String getCombinedPvcName() { return combinedPvcName; }

    public void setCombinedPvcName(String combinedPvcName) { this.combinedPvcName = combinedPvcName; }

    public String getArchivePathTranslation() { return archivePathTranslation; }

    public void setArchivePathTranslation(String archivePathTranslation) { this.archivePathTranslation = archivePathTranslation; }

    public String getBuildPathTranslation() { return buildPathTranslation; }

    public void setBuildPathTranslation(String buildPathTranslation) { this.buildPathTranslation = buildPathTranslation; }

    public String getCombinedPathTranslation() { return combinedPathTranslation; }

    public void setCombinedPathTranslation(String combinedPathTranslation) { this.combinedPathTranslation = combinedPathTranslation; }

    /**
     * Copies Slurm settings from a POJO. Keeping the stored token when a save omits it is handled before validation,
     * in DockerServerServiceImpl, so by this point the POJO carries the token to store.
     */
    private void updateSlurm(final SlurmSettings slurm) {
        if (slurm == null) {
            this.slurmRestUrl = null;
            this.slurmApiVersion = null;
            this.slurmUser = null;
            this.slurmToken = null;
            this.slurmPartition = null;
            this.slurmAccount = null;
            this.slurmQos = null;
            this.slurmTimeLimit = null;
            this.slurmRuntime = null;
            this.slurmSifDirectory = null;
            this.slurmAllowDockerPull = null;
            this.slurmPullCacheDirectory = null;
            return;
        }
        this.slurmRestUrl = slurm.restUrl();
        this.slurmApiVersion = slurm.apiVersion();
        this.slurmUser = slurm.user();
        this.slurmPartition = slurm.partition();
        this.slurmAccount = slurm.account();
        this.slurmQos = slurm.qos();
        this.slurmTimeLimit = slurm.timeLimit();
        this.slurmRuntime = slurm.runtime();
        this.slurmSifDirectory = slurm.sifDirectory();
        this.slurmAllowDockerPull = slurm.allowDockerPull();
        this.slurmPullCacheDirectory = slurm.pullCacheDirectory();
        this.slurmToken = slurm.token();
    }

    public String getSlurmRestUrl() { return slurmRestUrl; }

    public void setSlurmRestUrl(String slurmRestUrl) { this.slurmRestUrl = slurmRestUrl; }

    public String getSlurmApiVersion() { return slurmApiVersion; }

    public void setSlurmApiVersion(String slurmApiVersion) { this.slurmApiVersion = slurmApiVersion; }

    @Column(name = "slurm_user")
    public String getSlurmUser() { return slurmUser; }

    public void setSlurmUser(String slurmUser) { this.slurmUser = slurmUser; }

    @Column(columnDefinition = "text")
    public String getSlurmToken() { return slurmToken; }

    public void setSlurmToken(String slurmToken) { this.slurmToken = slurmToken; }

    public String getSlurmPartition() { return slurmPartition; }

    public void setSlurmPartition(String slurmPartition) { this.slurmPartition = slurmPartition; }

    public String getSlurmAccount() { return slurmAccount; }

    public void setSlurmAccount(String slurmAccount) { this.slurmAccount = slurmAccount; }

    public String getSlurmQos() { return slurmQos; }

    public void setSlurmQos(String slurmQos) { this.slurmQos = slurmQos; }

    public String getSlurmTimeLimit() { return slurmTimeLimit; }

    public void setSlurmTimeLimit(String slurmTimeLimit) { this.slurmTimeLimit = slurmTimeLimit; }

    public String getSlurmRuntime() { return slurmRuntime; }

    public void setSlurmRuntime(String slurmRuntime) { this.slurmRuntime = slurmRuntime; }

    public String getSlurmSifDirectory() { return slurmSifDirectory; }

    public void setSlurmSifDirectory(String slurmSifDirectory) { this.slurmSifDirectory = slurmSifDirectory; }

    public Boolean getSlurmAllowDockerPull() { return slurmAllowDockerPull; }

    public void setSlurmAllowDockerPull(Boolean slurmAllowDockerPull) { this.slurmAllowDockerPull = slurmAllowDockerPull; }

    public String getSlurmPullCacheDirectory() { return slurmPullCacheDirectory; }

    public void setSlurmPullCacheDirectory(String slurmPullCacheDirectory) { this.slurmPullCacheDirectory = slurmPullCacheDirectory; }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final DockerServerEntity that = (DockerServerEntity) o;

        boolean constrEqual = this.swarmConstraints == null && that.swarmConstraints == null;
        if (!constrEqual) {
            constrEqual = true;
            if (this.swarmConstraints == null || that.swarmConstraints == null ||
                    this.swarmConstraints.size() != that.swarmConstraints.size()) {
                constrEqual = false;
            } else {
                for (DockerServerEntitySwarmConstraint c : this.swarmConstraints) {
                    if (!that.swarmConstraints.contains(c)) {
                        constrEqual = false;
                        break;
                    }
                }
                if (constrEqual) {
                    for (DockerServerEntitySwarmConstraint c : that.swarmConstraints) {
                        if (!this.swarmConstraints.contains(c)) {
                            constrEqual = false;
                            break;
                        }
                    }
                }
            }
        }

        boolean tolEqual = this.kubernetesTolerations == null && that.kubernetesTolerations == null;
        if (!tolEqual) {
            tolEqual = true;
            if (this.kubernetesTolerations == null || that.kubernetesTolerations == null ||
                    this.kubernetesTolerations.size() != that.kubernetesTolerations.size()) {
                tolEqual = false;
            } else {
                for (DockerServerEntityKubernetesToleration t : this.kubernetesTolerations) {
                    if (!that.kubernetesTolerations.contains(t)) {
                        tolEqual = false;
                        break;
                    }
                }
                if (tolEqual) {
                    for (DockerServerEntityKubernetesToleration t : that.kubernetesTolerations) {
                        if (!this.kubernetesTolerations.contains(t)) {
                            tolEqual = false;
                            break;
                        }
                    }
                }
            }
        }

        return backend == that.backend &&
                Objects.equals(this.name, that.name) &&
                Objects.equals(this.host, that.host) &&
                Objects.equals(this.certPath, that.certPath) &&
                Objects.equals(this.pathTranslationXnatPrefix, that.pathTranslationXnatPrefix) &&
                Objects.equals(this.pathTranslationDockerPrefix, that.pathTranslationDockerPrefix) &&
                Objects.equals(this.pullImagesOnXnatInit, that.pullImagesOnXnatInit) &&
                Objects.equals(this.containerUser, that.containerUser) &&
                Objects.equals(this.autoCleanup, that.autoCleanup) &&
                Objects.equals(this.maxConcurrentFinalizingJobs, that.maxConcurrentFinalizingJobs) &&
                Objects.equals(this.statusEmailEnabled, that.statusEmailEnabled) &&
                Objects.equals(this.gpuVendor, that.gpuVendor) &&
                Objects.equals(this.archivePvcName, that.archivePvcName) &&
                Objects.equals(this.buildPvcName, that.buildPvcName) &&
                Objects.equals(this.combinedPvcName, that.combinedPvcName) &&
                Objects.equals(this.archivePathTranslation, that.archivePathTranslation) &&
                Objects.equals(this.buildPathTranslation, that.buildPathTranslation) &&
                Objects.equals(this.combinedPathTranslation, that.combinedPathTranslation) &&
                Objects.equals(this.buildDirCleanupEnabled, that.buildDirCleanupEnabled) &&
                Objects.equals(this.buildDirRetainDaysCompleted, that.buildDirRetainDaysCompleted) &&
                Objects.equals(this.buildDirRetainDaysFailed, that.buildDirRetainDaysFailed) &&
                Objects.equals(this.buildDirRetainDaysKilled, that.buildDirRetainDaysKilled) &&
                Objects.equals(this.buildDirCleanupTime, that.buildDirCleanupTime) &&
                Objects.equals(this.slurmRestUrl, that.slurmRestUrl) &&
                Objects.equals(this.slurmApiVersion, that.slurmApiVersion) &&
                Objects.equals(this.slurmUser, that.slurmUser) &&
                Objects.equals(this.slurmToken, that.slurmToken) &&
                Objects.equals(this.slurmPartition, that.slurmPartition) &&
                Objects.equals(this.slurmAccount, that.slurmAccount) &&
                Objects.equals(this.slurmQos, that.slurmQos) &&
                Objects.equals(this.slurmTimeLimit, that.slurmTimeLimit) &&
                Objects.equals(this.slurmRuntime, that.slurmRuntime) &&
                Objects.equals(this.slurmSifDirectory, that.slurmSifDirectory) &&
                Objects.equals(this.slurmAllowDockerPull, that.slurmAllowDockerPull) &&
                Objects.equals(this.slurmPullCacheDirectory, that.slurmPullCacheDirectory) &&
                constrEqual &&
                tolEqual;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, host, certPath, lastEventCheckTime, backend, pathTranslationXnatPrefix,
                pathTranslationDockerPrefix, pullImagesOnXnatInit, containerUser, autoCleanup, swarmConstraints,
                maxConcurrentFinalizingJobs, statusEmailEnabled, gpuVendor, archivePvcName, buildPvcName, combinedPvcName,
                archivePathTranslation, buildPathTranslation, combinedPathTranslation, kubernetesTolerations,
                buildDirCleanupEnabled, buildDirRetainDaysCompleted, buildDirRetainDaysFailed,
                buildDirRetainDaysKilled, buildDirCleanupTime,
                slurmRestUrl, slurmApiVersion, slurmUser, slurmToken, slurmPartition, slurmAccount, slurmQos, slurmTimeLimit, slurmRuntime, slurmSifDirectory, slurmAllowDockerPull, slurmPullCacheDirectory);
    }

}
