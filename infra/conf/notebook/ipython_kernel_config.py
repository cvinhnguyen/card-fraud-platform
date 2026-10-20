# IPython kernel config of the notebook service, mounted at /etc/ipython (read by every kernel).
# Output written straight to the process's stdout / stderr by the JVM and by Spark's Python workers (log4j lines,
# JVM startup notices, XGBoost worker logs) goes to the container log (docker logs notebook) instead of the cells:
# the cells keep what the notebook code prints. Python warnings and exceptions (Java ones included) still show.
c = get_config()  # type: ignore  # noqa: F821  (defined by IPython when it loads this file)
c.IPKernelApp.capture_fd_output = False
