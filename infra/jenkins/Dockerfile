# Jenkins controller of the platform's CI/CD. Pipelines (a Jenkinsfile next to each component) run on it and drive
# the host's Docker through its socket (build, run tests in containers, push to GHCR, ghcr.io/cvinhnguyen) and the
# kind cluster through a deploy-only ServiceAccount (helm, kubectl). cosign signs the pushed images with the registry
# login of `docker login` (infra/k8s/supply-chain.sh). Configured as code: casc.yaml (users, credentials, jobs).
FROM docker:29.7.2-cli AS docker
FROM jenkins/jenkins:2.580.1-lts-jdk21
USER root
COPY --from=docker /usr/local/bin/docker /usr/local/bin/docker
COPY --from=docker /usr/local/libexec/docker/cli-plugins /usr/local/libexec/docker/cli-plugins
ARG KUBECTL=v1.37.0 HELM=v3.22.1 COSIGN=v2.6.5
RUN apt-get update && apt-get install -y --no-install-recommends jq && rm -rf /var/lib/apt/lists/* \
    && curl -fsSL "https://dl.k8s.io/release/${KUBECTL}/bin/linux/amd64/kubectl" -o /usr/local/bin/kubectl \
    && curl -fsSL "https://github.com/sigstore/cosign/releases/download/${COSIGN}/cosign-linux-amd64" -o /usr/local/bin/cosign \
    && chmod +x /usr/local/bin/kubectl /usr/local/bin/cosign \
    && curl -fsSL "https://get.helm.sh/helm-${HELM}-linux-amd64.tar.gz" | tar -xz -C /usr/local/bin --strip-components=1 linux-amd64/helm
USER jenkins
COPY --chown=jenkins:jenkins plugins.txt /usr/share/jenkins/ref/plugins.txt
RUN jenkins-plugin-cli --plugin-file /usr/share/jenkins/ref/plugins.txt
# no setup wizard: casc.yaml creates the admin user and the jobs. An aborted build gets 15 minutes before Jenkins
# terminates its steps (default: seconds), so the rollback in post finishes: the longest, card-support-mcp, is three
# helm rollbacks of up to 4 minutes. "Forcibly terminate" in the UI still stops a build at once.
ENV JAVA_OPTS="-Djenkins.install.runSetupWizard=false -Xmx512m \
-Dorg.jenkinsci.plugins.workflow.job.WorkflowRun.termGracePeriod=900s \
-Dorg.jenkinsci.plugins.workflow.job.WorkflowRun.killGracePeriod=960s" CASC_JENKINS_CONFIG=/var/jenkins_home/casc.yaml
