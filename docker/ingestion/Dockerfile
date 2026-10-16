# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,

FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /build
COPY contracts /build/contracts
COPY ingestion /build/ingestion
RUN --mount=type=cache,target=/root/.m2 \
    mvn -B -q -f ingestion/pom.xml package

FROM eclipse-temurin:17-jre
RUN groupadd --system fbi && useradd --system --gid fbi --home /opt/fbi fbi
WORKDIR /opt/fbi
COPY --from=build /build/ingestion/ingestion-sink/target/fineract-bi-sink.jar /opt/fbi/sink.jar
COPY --from=build /build/ingestion/ingestion-loadgen/target/fineract-bi-loadgen.jar /opt/fbi/loadgen.jar
COPY contracts /opt/fbi/contracts
ENV FBI_CONTRACT_PATH=/opt/fbi/contracts/fineract_tables.json \
    JAVA_OPTS="-XX:MaxRAMPercentage=75 -XX:+UseG1GC -XX:+ExitOnOutOfMemoryError"
USER fbi
EXPOSE 9404
ENTRYPOINT ["sh", "-c", "if [ \"$1\" = loadgen ]; then shift; exec java $JAVA_OPTS -jar /opt/fbi/loadgen.jar \"$@\"; else exec java $JAVA_OPTS -jar /opt/fbi/sink.jar \"$@\"; fi", "--"]
CMD ["run"]
