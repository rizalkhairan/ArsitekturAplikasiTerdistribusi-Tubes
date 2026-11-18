FROM citusdata/citus:13.0.1-pg16@sha256:b5417130b32f5739dfb3b2f92bc3d6620a4d5514b8235fce62143128128cf85a
USER root
RUN apt-get update && apt-get install -y --no-install-recommends python3-venv \
    && python3 -m venv /opt/patroni \
    && /opt/patroni/bin/pip install --no-cache-dir 'patroni[kubernetes]==4.1.5' 'psycopg2-binary==2.9.11' \
    && useradd --uid 1000 --create-home canonical \
    && rm -rf /var/lib/apt/lists/*
COPY start.py bootstrap.py recovery.py /opt/canonical/
ENV PATH=/opt/patroni/bin:$PATH
USER 1000:1000
ENTRYPOINT ["python3", "/opt/canonical/start.py"]
