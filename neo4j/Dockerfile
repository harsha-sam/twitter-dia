FROM neo4j:5.19-community-bullseye

COPY db-init.sh /db-init.sh

RUN chmod +x /db-init.sh

CMD ["/db-init.sh"]