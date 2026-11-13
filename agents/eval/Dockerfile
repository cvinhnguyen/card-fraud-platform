# Runner of the agents' evaluations (run_eval.py, langfuse_gate.py), used by the CD pipelines and by hand.
FROM python:3.13-slim
RUN pip install --no-cache-dir langfuse==5.0.3
WORKDIR /eval
COPY dataset.jsonl run_eval.py langfuse_gate.py test_run_eval.py ./
RUN python test_run_eval.py   # the scoring itself is checked before the image can gate anything
ENTRYPOINT ["python"]
