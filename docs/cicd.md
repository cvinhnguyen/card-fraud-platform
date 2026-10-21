# CI/CD with Jenkins

Five Jenkins pipelines build, test and deploy the Layer 3 components. Each one is a Jenkinsfile next to the code it
ships, and each one ends with a check against the running platform, so a green build means the new version answers.

| Jenkins job | Jenkinsfile | Runs when | Ships | Rubric |
|---|---|---|---|---|
| `rag-pipeline` | [`pipelines/rag/Jenkinsfile`](../pipelines/rag/Jenkinsfile) | RAG steps, DAG, documents or ml image changed | image of the RAG steps, released to Airflow and run | C33 |
| `card-support-mcp` | [`services/card_support_mcp/Jenkinsfile`](../services/card_support_mcp/Jenkinsfile) | `services/card_support_mcp/**` changed | MCP server image, Helm release in `card-support` | B34 |
| `card-support-data-agents` | [`agents/Jenkinsfile.data-agents`](../agents/Jenkinsfile.data-agents) | `agents/**` changed, after the MCP pipeline, or a new production prompt in Langfuse | card-data-agent and risk-agent, Helm release in `kagent` | C34 |
| `card-support-coordinator` | [`agents/Jenkinsfile.coordinator`](../agents/Jenkinsfile.coordinator) | `agents/**` changed, after the data agents, or a new production prompt | coordinator, Helm release in `kagent` | C35 |
| `stream-feature-jobs` | [`ml/jobs/Jenkinsfile.stream`](../ml/jobs/Jenkinsfile.stream) | push jobs, ML library, Feast repo or ml image changed | push-online and push-offline (Kafka to Redis and Delta) | B36 |

## Jenkins setup

Jenkins 2.580.1 runs in compose (profile `cicd`, UI on `127.0.0.1:8095`, login required). The image
([`infra/jenkins/Dockerfile`](../infra/jenkins/Dockerfile)) adds the Docker CLI with buildx and compose, kubectl
v1.37.0, Helm v3.22.1 and jq, plus the plugins in [`plugins.txt`](../infra/jenkins/plugins.txt). Nothing is clicked
together in the UI: [`casc.yaml`](../infra/jenkins/casc.yaml) (configuration as code) creates the admin user, the
credentials and the five jobs. The jobs poll GitHub `main` every 5 minutes, because no webhook can reach this machine.
One executor runs one pipeline at a time, since two image builds do not fit in the machine's RAM.

```bash
sh infra/jenkins/setup.sh     # deploy ServiceAccount + its kubeconfig, then the Jenkins container (profile cicd)
```

Secrets never appear in a Jenkinsfile. They start in the gitignored `infra/.env` (or `infra/.secrets/` for files),
reach the container as Docker secrets under `/run/secrets/`, and JCasC turns each into a Jenkins credential. A
Jenkinsfile names the credential id inside `withCredentials` and Jenkins masks the value in the console log. The API
keys go to curl on stdin (`curl -K -`) and to containers through the environment (`docker run -e NAME`), so they do
not show up in the process list or the build log.

| Credential id | Used by |
|---|---|
| `kubeconfig` | every deploy (ServiceAccount `ci/jenkins-deployer`) |
| `langfuse-public-key`, `langfuse-secret-key` | agent pipelines: system prompts and the eval experiment |
| `a2a-key-coordinator`, `a2a-key-support-app` | A2A smoke tests through agentgateway |
| `edge-admin-password` | MCP smoke test of the Web API over HTTPS through NGINX |
| `cosign-key`, `cosign-password` | signing the MCP image |
| `ml-db-password`, `redis-password`, `s3-access-key`, `s3-secret-key` | stream jobs deploy and smoke test |
| `airflow-admin` | RAG pipeline: release and run through the Airflow REST API |

Jenkins deploys with its own ServiceAccount ([`infra/jenkins/rbac.yaml`](../infra/jenkins/rbac.yaml)), not with the
cluster admin. The role is bound only in `card-support`, `kagent` and `agentgateway-system`, the namespaces the MCP
and agents charts write to, so a pipeline cannot touch Vault, the monitoring stack or kube-system. `setup.sh` writes
its kubeconfig to `infra/.secrets/jenkins-kubeconfig`.

Every Jenkinsfile wraps its stages in a `when { changeset ... }` on its own paths. A poll that finds commits
elsewhere in the repo checks out and stops: these are the rows with only "Checkout SCM" in the stage views below.
Jenkins keeps the host's Docker socket because it builds images. Airflow only runs the RAG image, so it reaches
Docker through `docker-proxy` on an internal network, which allows the container and image API calls and denies exec,
build, volumes and the rest; the DAG itself only accepts a `fraud/ml-train` image as `rag_image`.

## RAG data pipeline

[`pipelines/rag/Jenkinsfile`](../pipelines/rag/Jenkinsfile) tests the steps of the Airflow DAG `rag_ingest`
(chunk the bank documents, embed the chunks, write them to the Feast knowledge base, validate), builds the image the
steps run in, releases that image to Airflow and runs the DAG. The DAG starts each step as a container of the image
named in the Airflow Variable `rag_image`, so releasing a new version is one Variable update. More on the pipeline
itself: [rag_pipeline.md](rag_pipeline.md).

![Jenkins stage view of rag-pipeline: build #2 ran every stage, later polls stopped after checkout](images/cicd_rag-pipeline.png)

| Stage | What it does |
|---|---|
| Version | short commit hash, used as the image tag |
| Unit tests | builds the `test` stage of [`ml/Dockerfile`](../ml/Dockerfile) and runs [`test_rag_ingest.py`](../ml/jobs/tests/test_rag_ingest.py) |
| DAG check | imports [`rag_ingest.py`](../pipelines/airflow/dags/rag_ingest.py) in the Airflow image and asserts its 5 tasks |
| Build image | `fraud/ml-train:<commit>` |
| Release + run | Airflow REST API: sets Variable `rag_image` to the new tag, unpauses the DAG, triggers a run, waits for `success` or `failed` |
| Validation results | prints the checks the `validate_rag.run_checks` task logged |

Build #2 (started by hand, since the pipeline needs the Airflow profile up): 6 unit tests passed, the DAG imported
with its 5 tasks, `rag_image = fraud/ml-train:8f40d6a` was released and run `ci-8f40d6a-1792528682` succeeded
(`chunk_documents` 4 s, `embed_chunks` 6 s, `write_feature_store` 10 s, `validate_rag.run_checks` 13 s,
`validate_rag.publish_results` 1 s). The checks it printed:

```
ok   expect_table_row_count_to_be_between         observed=57
ok   expect_column_values_to_be_unique            observed=0
ok   expect_column_values_to_not_be_null          observed=0
ok   expect_column_value_lengths_to_be_between    observed=0
ok   expect_embedding_dimension_to_equal          observed=[1024]
ok   expect_every_document_to_have_chunks         observed=['authentication_rules', 'card_terms', 'dispute_policy', 'faq', 'fee_schedule']
ok   expect_online_store_to_match_offline         observed=57/57 chunks identical online
```

## MCP server

[`services/card_support_mcp/Jenkinsfile`](../services/card_support_mcp/Jenkinsfile) ships the card support Web API /
MCP server, the tool server both data agents use. When it succeeds it starts the data agents' pipeline, so the agents
are deployed against the MCP release that just passed.

![Jenkins stage view of card-support-mcp: #9 failed at signing, #10 and #11 passed every stage](images/cicd_card-support-mcp.png)

| Stage | What it does |
|---|---|
| Version | short commit hash, used as the image tag |
| Unit tests + coverage gate | `docker build --target test`, then pytest in it; fails under 90% coverage ([testing.md](testing.md#layer-3-the-card-support-mcp-server)) |
| Mutation test | mutmut on the tool rules; fails under 80% of mutants killed |
| Build + push image | runtime stage of the multi-stage Dockerfile, pushed to the compose registry as `card-support-mcp:<commit>` |
| Sign image (cosign) | signs the image by digest with the CI key ([novel_supply_chain.md](novel_supply_chain.md)) |
| Deploy (helm --atomic) | `helm upgrade --install --atomic` into `card-support`: rolling update with maxUnavailable 0, and a release whose pods never get Ready is rolled back |
| Smoke test | the Web API over HTTPS through NGINX must return a real card, and both kagent RemoteMCPServers must accept the new release |
| post: success | `build job: 'card-support-data-agents'` |

Build #9 failed at "Sign image": the signing key file was not readable by the Jenkins user, so cosign got an empty
key (`reading key: invalid pem block`). Deploy and smoke test never ran and the cluster kept the release it had. After
the key file was fixed, build #10 passed every stage: 97 tests at 97.84% coverage, 243 of 245 mutants killed, image
`a1d4e12` pushed and signed (`sha256:8f57ef0f...`), Helm revision 11 rolled out, `Web API: card data OK over HTTPS`,
both RemoteMCPServers `Accepted`, then `Scheduling project: card-support-data-agents`. Build #11 (started by hand
after the trigger fix below) did the same in 89 s: 97 tests, image `b5bd125` signed (`sha256:fdb05642...`), Helm
revision 12, and it started data agents #29.

## Agents

The agents' system prompts live in Langfuse (prompt `card-support/<agent>`, label `production`).
[`agents/fetch_prompts.sh`](../agents/fetch_prompts.sh) pulls each one into `agents/chart/prompts/<agent>.md` and
returns Helm flags with the prompt versions, which the chart writes into an annotation on each Agent. Every 15
minutes both agent pipelines compare the deployed versions with production. Promoting a new prompt version in
Langfuse therefore redeploys the agent without a commit, and a run with nothing new stops after this check.

The chain for a tool change is MCP server, then data agents, then coordinator: each pipeline starts the next one when
it succeeds (the data agents only when they deployed something).

This chain did not work at first. The `Changed?` stage of both agent pipelines tested `triggeredBy 'UpstreamCause'`,
but the `build` step records a `BuildUpstreamCause`, so the runs started by an upstream pipeline (data agents #28
after MCP #10, coordinator #11, #19 and #21) checked the prompts and stopped without deploying. The running agents
still used the new MCP release, because its service address and tool names stayed the same, but the redeploy and the
eval gate that should follow a tool change did not run. Commit `b5bd125` added `triggeredBy 'BuildUpstreamCause'` to
both agent Jenkinsfiles. The next chain ran end to end: MCP #11, then data agents #29 ("Started by upstream project
card-support-mcp build number 11": deploy, smoke test, eval gate passed), then coordinator #26. Coordinator #25 had
already built that commit, so #26 had no change to deploy; it deployed and ran its smoke test only because data
agents #29 started it, which is the case the fix is for.

### Data agents

[`agents/Jenkinsfile.data-agents`](../agents/Jenkinsfile.data-agents) deploys card-data-agent and risk-agent (Helm
release `card-support-data-agents`).

![Jenkins stage view of card-support-data-agents: #29, started by the MCP pipeline, deployed and passed the eval gate; #27 failed in the eval gate](images/cicd_card-support-data-agents.png)

| Stage | What it does |
|---|---|
| Prompts from Langfuse | fetches the production prompts, compares their versions with the deployed ones |
| Changed? | also deploys on a change under `agents/` or a manual / upstream start |
| Validate | `helm lint`, then `helm template` through a server-side dry run against the cluster |
| Deploy | `helm upgrade --atomic`, then waits until both Agents are Ready |
| Smoke test (A2A through the gateway) | asks each agent one question through agentgateway, the way the coordinator calls it (coordinator key, header `x-kagent-host`); the answer must complete and contain the expected text |
| Eval gate (Langfuse experiment) | the risk agent answers the eval dataset as a Langfuse experiment; below 0.6 accuracy, `helm rollback` and the build fails ([novel_eval_gate.md](novel_eval_gate.md)) |

- #20 (a commit under `agents/`): deploy and smoke test passed, the eval scored 0.583 against the 0.6 gate, Helm rolled
  the release back and the build failed.
- #23 (risk-agent prompt v2 promoted in Langfuse): the first stage logged `risk-agent=1` deployed and `risk-agent=2` in
  production, so it deployed v2. Smoke answers: `The card made 3 payments in the last 30 days.` and the foreign fee
  with its source `(fee_schedule#2)`. Eval 0.917, pass. Builds #20 and #23 ran the 12-question dataset once; the gate
  now asks 20 questions twice.
- #27: Jenkins was restarted while the eval container ran (to load the corrected cosign credential), the shell step
  lost its process and returned an error. The gate treats that like a failed eval: it rolled the release back to the
  revision that was running before (prompt v2 still in place) and failed the build.
- #29 (started by MCP #11, with the trigger fix): deploy and smoke test passed, then the eval gate asked the 20
  questions twice: 0.95 and 0.90, `accuracy 0.93 over 2 run(s) of 20 questions (gate 0.60): pass`. It then started the
  coordinator pipeline.

### Coordinator

[`agents/Jenkinsfile.coordinator`](../agents/Jenkinsfile.coordinator) deploys the coordinator (Helm release
`card-support-coordinator`) with the same first four stages. Its smoke test calls it the way a client application
does: the public A2A route `/a2a/coordinator` with the support-app key, and a question that needs both data agents.

![Jenkins stage view of card-support-coordinator: #24, #25 and #26 deployed and passed the client smoke test; #26 had no new commit and was started by data agents #29](images/cicd_card-support-coordinator.png)

| Stage | What it does |
|---|---|
| Prompt from Langfuse | fetches the coordinator's production prompt, compares the version with the deployed one |
| Changed? | also deploys on a change under `agents/` or a manual / upstream start |
| Validate | `helm lint` and a server-side dry run |
| Deploy | `helm upgrade --atomic`, waits until the coordinator is Ready |
| Smoke test (A2A client through the gateway) | one question needing card data and policy; the answer must complete and cite `fee_schedule` |

Build #24 (commit under `agents/`): `coordinator: completed | The card made 3 payments in the last 30 days. The
foreign transaction fee is 2.5% for debit cards and 3.0% for credit cards (fee_schedule#2).` Build #26, started by
data agents #29, answered the same question the same way. Build #18 failed its
smoke test while the LLM canary sent part of the traffic to Groq's free tier, which answered 429; that is why the
canary was stopped ([ab_testing.md](ab_testing.md)).

## Stream feature jobs

[`ml/jobs/Jenkinsfile.stream`](../ml/jobs/Jenkinsfile.stream) ships the two push jobs that write the streaming
features to the feature store: `push-online` (Kafka to Redis) and `push-offline` (Kafka to the Delta table behind the
push source). They run as compose services on the ml image.

![Jenkins stage view of stream-feature-jobs: build #2 ran every stage, later polls stopped after checkout](images/cicd_stream-feature-jobs.png)

| Stage | What it does |
|---|---|
| Version | short commit hash, used as the image tag |
| Unit tests | the `test` stage of [`ml/Dockerfile`](../ml/Dockerfile): tests of the push jobs and the RAG steps |
| Build image | `fraud/ml-train:<commit>` and `:latest` |
| Deploy | starts Kafka if it is down, recreates `push-online` and `push-offline` on the new image |
| Smoke test | [`ml/tools/stream_smoke.py`](../ml/tools/stream_smoke.py) sends one marked authorization summary (`ci_smoke_<random>`, never a real card) to Kafka; it must reach the online store and the offline table within 180 s |

Build #2 (started by hand): 15 tests passed, both containers were recreated on the new image, and the smoke message
arrived in both stores: `ci_smoke_fabe0b34: online 12.4 s, offline 8.4 s (s3://lakehouse/gold/feat_card_auth_rt)`.
