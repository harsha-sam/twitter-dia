FROM confluentinc/cp-zookeeper:latest

USER root

RUN mkdir -p /usr/scripts
WORKDIR /usr/scripts

COPY --chown=root . /usr/scripts/

RUN chmod +x /usr/scripts/topics-init.sh

ENTRYPOINT ["./topics-init.sh"]