FROM alpine

ENV JAVA_HOME=/usr/lib/jvm/default-jvm/jre
ENV PDI_VERSION=7.1 \
    PDI_BUILD=7.1.0.0-12 \
    JRE_HOME=${JAVA_HOME} \
    PENTAHO_JAVA_HOME=${JAVA_HOME} \
    PENTAHO_HOME=/opt/pentaho \
    KETTLE_HOME=/opt/pentaho/data-integration\
    PATH=${PATH}:${JAVA_HOME}/bin

RUN echo "Install dependencies..." && \
    apk update && \
    apk add openjdk8-jre bash && \
    apk add ca-certificates wget && \
    update-ca-certificates

RUN echo "Install pentaho ..." && \
    mkdir -p ${PENTAHO_HOME} && \
    wget -qO /tmp/pdi-ce.zip https://downloads.sourceforge.net/project/pentaho/Data%20Integration/${PDI_VERSION}/pdi-ce-${PDI_BUILD}.zip && \
    unzip -q /tmp/pdi-ce.zip -d ${PENTAHO_HOME} && \
    rm -f /tmp/pdi-ce.zip && \
    chmod -R g+w ${PENTAHO_HOME}

RUN echo "Cleaning unused files..." && \
    cd ${KETTLE_HOME} && \
    rm -rf libswt/win* libswt/osx64/ docs/ samples/ && \
    find -name "*.bat" -delete && \
	  find -name "*.exe" -delete
# TODO: UI folder??

RUN mkdir ${PENTAHO_HOME}/repository/
#  && \
#     mkdir -p ${PENTAHO_HOME}/repository/jobs && \
#     mkdir -p ${PENTAHO_HOME}/repository/transformations

COPY etl ${PENTAHO_HOME}/repository/
# RUN useradd -ms /bin/bash pentaho
# USER pentaho
# WORKDIR /home/pentaho


# VOLUME ["/opt/pentaho/repository"]
EXPOSE 8080

WORKDIR $KETTLE_HOME

