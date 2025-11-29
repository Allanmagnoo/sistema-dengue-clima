FROM astrocrpublic.azurecr.io/runtime:3.1-5

# Install Java (Required for PySpark)
USER root
RUN apt-get update && \
    apt-get install -y default-jdk procps && \
    apt-get clean

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/default-java
USER astro
