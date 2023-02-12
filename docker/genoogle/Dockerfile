FROM alpine:latest
LABEL maintainer="gomes.rsantanna@gmail.com"

# Environment Setup
ENV JAVA_HOME /usr/lib/jvm/default-jvm
ENV GENOOGLE_HOME=/app/genoogle

WORKDIR ${GENOOGLE_HOME}

# Install necessary packages
RUN apk -U upgrade && \
    apk add --no-cache bash git openjdk8 apache-ant

# Get the latest Genoogle and remove default configuration and example files
RUN git clone https://github.com/rsantanna/Genoogle.git . && \
    ant jar && \
    mv ./ant-build/genoogle.jar . && \
    rm -r ./conf && \
    rm -r ./files/fasta

# Run
ENTRYPOINT ["/bin/bash"]