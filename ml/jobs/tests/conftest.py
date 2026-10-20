"""The jobs are scripts, not a package: import them from ml/jobs (and fraud_ml from ml/)."""
import os
import sys

JOBS = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path[:0] = [JOBS, os.path.dirname(JOBS)]
