"""Layer 2: model and data drift, every day, from the offline store and the ground truth; retrain on drift.

| Task | What it does |
|---|---|
| `compute_drift` | `ml/jobs/drift_job.py`: the last 7 days of payments with their ground truth, point-in-time features from the Feast offline store, scored by the `production` model; PR-AUC / recall at 1% FPR / fraud rate, and PSI of every feature and of the score vs the model's reference profile, logged as an MLflow run (experiment `card_fraud_monitoring`) |
| `decide` | retrain when any PSI > `drift_psi_threshold` or PR-AUC fell more than `drift_pr_auc_drop` below the model's test PR-AUC |
| `trigger_retraining` | starts a run of the released Kubeflow pipeline version through the KFP API, `label_as_of` = this run's day. The release is the Airflow Variable `train_release` (pipeline and version ids, image), set by `ml/pipelines/Jenkinsfile`; before the first release it submits the package compiled in the repo, `ml/pipelines/train_fraud_model.yaml`. The new model still has to pass evaluation and review before it serves |
| `no_drift` | nothing to do |
"""
import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

from common import CODE, FEAST, FEAST_ENV, RELEASE_TAG

with DAG("drift_monitoring", schedule="@daily", start_date=datetime(2026, 10, 20), catchup=False, max_active_runs=1,
         default_args={"owner": "ml-platform", "retries": 1, "retry_delay": timedelta(minutes=2)},
         doc_md=__doc__, tags=["layer2", "monitoring", RELEASE_TAG]) as dag:
    compute = BashOperator(
        task_id="compute_drift", pool="spark", append_env=True, do_xcom_push=True,
        env={**FEAST_ENV, "MLFLOW_TRACKING_URI": "{{ conn.mlflow.host }}", "FEAST_REPO": "{{ var.value.feast_repo }}"},
        bash_command=f"set -o pipefail; {FEAST}/python {CODE}/ml/jobs/drift_job.py --end {{{{ ds }}}} --days 7 "
                     "--psi-threshold {{ var.value.drift_psi_threshold }} --max-drop {{ var.value.drift_pr_auc_drop }}"
                     " | tail -1")   # the last line is the JSON verdict -> XCom

    @task.branch(task_id="decide")
    def decide(verdict: str) -> str:
        """Branch on the job's verdict."""
        v = json.loads(verdict)
        print(f"model v{v['model_version']}: PR-AUC {v['pr_auc']} (test {v['test_pr_auc']}), "
              f"worst PSI {v['worst_psi']} on {v['worst_input']} -> retrain={v['retrain']}")
        return "trigger_retraining" if v["retrain"] else "no_drift"

    # kfp lives in the feast venv, not in Airflow's own environment
    @task.external_python(task_id="trigger_retraining", python=f"{FEAST}/python", expect_airflow=False)
    def trigger_retraining(host: str, label_as_of: str, release: str, package: str) -> str:
        """Start train_fraud_model on KFP with labels as of today (not named `ds`: Airflow reserves its context keys):
        the released pipeline version when there is one, else the package compiled in the repo."""
        import json

        import kfp
        client, name = kfp.Client(host=host), f"drift-retrain-{label_as_of}"
        if release:
            r = json.loads(release)
            experiment = client.create_experiment("card_fraud")   # returns the experiment if it exists
            run = client.run_pipeline(experiment_id=experiment.experiment_id, job_name=name,
                                      params={"label_as_of": label_as_of}, pipeline_id=r["pipeline_id"],
                                      version_id=r["version_id"], enable_caching=False)
            print(f"KFP run {run.run_id}: pipeline version {r['version']}, image {r['image']}")
        else:
            run = client.create_run_from_pipeline_package(package, arguments={"label_as_of": label_as_of}, run_name=name,
                                                          experiment_name="card_fraud", enable_caching=False)
            print(f"KFP run {run.run_id}: package {package} (no released pipeline version yet)")
        return run.run_id

    decide(compute.output) >> [trigger_retraining("{{ conn.kfp_api.host }}", "{{ ds }}",
                                                  "{{ var.value.get('train_release', '') }}",
                                                  f"{CODE}/ml/pipelines/train_fraud_model.yaml"),
                               EmptyOperator(task_id="no_drift")]
