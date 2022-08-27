FROM python:3.10-slim

ARG TESTING=0

RUN apt-get update

# copy files
COPY README.md app/README.md
COPY requirements.txt app/requirements.txt

# install requirements
RUN pip install -r app/requirements.txt

# copy library files
COPY nowcasting_metrics/ app/nowcasting_metrics/
COPY tests/ app/tests/

# change to app folder
WORKDIR /app

# Add python pathh library
ENV PYTHONPATH=${PYTHONPATH}:/app/nowcasting_metrics

RUN if [ "$TESTING" = 1 ]; then pip install pytest pytest-cov coverage; fi

CMD ["python", "-u","nowcasting_metrics/app.py"]
