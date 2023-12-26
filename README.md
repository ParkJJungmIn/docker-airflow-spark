# Installation

```
# download docker compose file
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.6.2/docker-compose.yaml'

# start docker compose
docker compose up

$ docker ps
CONTAINER ID   IMAGE                  COMMAND                  CREATED          STATUS                             PORTS                    NAMES
03b8b349b8fb   apache/airflow:2.6.2   "/usr/bin/dumb-init …"   46 seconds ago   Up 26 seconds (health: starting)   8080/tcp                 airflow_study-airflow-scheduler-1
687ef46e6085   apache/airflow:2.6.2   "/usr/bin/dumb-init …"   46 seconds ago   Up 25 seconds (health: starting)   8080/tcp                 airflow_study-airflow-worker-1
c55fcc3e773a   apache/airflow:2.6.2   "/usr/bin/dumb-init …"   46 seconds ago   Up 25 seconds (health: starting)   8080/tcp                 airflow_study-airflow-triggerer-1
bd91be45d198   postgres:13            "docker-entrypoint.s…"   46 seconds ago   Up 45 seconds (healthy)            0.0.0.0:5432->5432/tcp   airflow_study-postgres-1
4fe735b3990c   redis:latest           "docker-entrypoint.s…"   46 seconds ago   Up 45 seconds (healthy)            6379/tcp                 airflow_study-redis-1

# credential
username: airflow / password: airflow
```