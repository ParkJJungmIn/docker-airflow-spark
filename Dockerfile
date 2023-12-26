# Dockerfile 이름을 Dockerfile로 하고, docker-compose.yaml 파일과 같은 디렉토리에 저장합니다.
FROM apache/airflow:2.6.2

USER root

# OpenJDK 설치
RUN apt-get update \
    && apt-get install -y openjdk-11-jdk procps\
    && rm -rf /var/lib/apt/lists/* 

# JAVA_HOME 환경변수 설정
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-arm64
ENV PATH $JAVA_HOME/bin:$PATH

USER airflow
