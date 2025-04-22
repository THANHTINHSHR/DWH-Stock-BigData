FROM apache/superset:latest

USER root

RUN pip install --no-cache-dir --disable-pip-version-check --no-deps --trusted-host pypi.org --trusted-host pypi.python.org --trusted-host files.pythonhosted.org PyAthena Pillow flask-cors

USER superset
