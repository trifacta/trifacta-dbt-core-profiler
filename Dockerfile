FROM python:3.8-slim AS builder

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

COPY requirements.txt .
COPY libs/ libs/
COPY trifacta-dbt/ /opt/venv/trifacta-dbt/

RUN /opt/venv/bin/python -m pip install --upgrade pip

RUN pip install -r requirements.txt

FROM python:3.8-slim AS run-image
COPY --from=builder /opt/venv /opt/venv

# Make sure we use the virtualenv:
ENV PATH="/opt/venv/bin:$PATH"

VOLUME ["/home"]

WORKDIR /home

ENTRYPOINT ["/opt/venv/bin/python3", "/opt/venv/trifacta-dbt/main.py"]
