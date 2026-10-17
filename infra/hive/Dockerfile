# Hive Metastore 3.1.3 (Spark 4.1's built-in Hive 2.3 client cannot call a 4.x metastore).
FROM apache/hive:3.1.3

USER root
# The bundled Postgres driver (9.4) predates SCRAM auth, the Postgres 16 default.
ADD --chown=hive:hive https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.14/postgresql-42.7.14.jar /opt/hive/lib/
# S3A, so the metastore can check table locations on s3a://lakehouse/...
RUN rm /opt/hive/lib/postgresql-9.4.1208.jre7.jar \
 && ln -s /opt/hadoop/share/hadoop/tools/lib/hadoop-aws-3.1.0.jar /opt/hadoop/share/hadoop/tools/lib/aws-java-sdk-bundle-1.11.271.jar /opt/hive/lib/
USER hive

# The stock entrypoint runs `schematool -initSchema` on every start and exits once the schema exists.
ENTRYPOINT ["bash", "-c", "/opt/hive/bin/schematool -dbType postgres -info >/dev/null 2>&1 && export IS_RESUME=true; exec /entrypoint.sh"]
