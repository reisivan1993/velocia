# Apache Gluten (Velox backend) engine image for Velocia: vanilla Spark 3.5.9 plus the Gluten
# bundle jar. The official binary release is verified by checksum. Its native libraries are built
# for linux/amd64 only, so the image is amd64 even when built on an arm64 machine.
FROM --platform=linux/amd64 spark@sha256:d40624d060dcb1ecc5b19311f4ec121b3a81d8f797950eaacd27f0585053a4b9 AS fetch

ARG GLUTEN_VERSION=1.7.0
ADD --checksum=sha256:ac76a774539f67079e9c9b2eea3b8a693cad7f371ecdae371352eb9ffbaad05c \
    https://downloads.apache.org/gluten/${GLUTEN_VERSION}/apache-gluten-${GLUTEN_VERSION}-bin-spark-3.5.tar.gz \
    /tmp/gluten.tar.gz
USER root
RUN mkdir /tmp/gluten && tar -xzf /tmp/gluten.tar.gz -C /tmp/gluten --strip-components=1

FROM --platform=linux/amd64 spark@sha256:d40624d060dcb1ecc5b19311f4ec121b3a81d8f797950eaacd27f0585053a4b9

ARG GLUTEN_VERSION=1.7.0
COPY --from=fetch --chmod=644 \
    /tmp/gluten/gluten-velox-bundle-spark3.5_2.12-linux_amd64-${GLUTEN_VERSION}.jar /opt/spark/jars/
COPY --from=fetch --chmod=644 /tmp/gluten/NOTICE /opt/spark/licenses/gluten-NOTICE
COPY --from=fetch --chmod=644 /tmp/gluten/LICENSE /opt/spark/licenses/gluten-LICENSE

LABEL org.opencontainers.image.title="velocia-engine-spark-gluten" \
      org.opencontainers.image.description="Apache Spark 3.5.9 with Apache Gluten 1.7.0 (Velox) for Velocia" \
      org.opencontainers.image.licenses="Apache-2.0" \
      dev.velocia.engine="spark-gluten" \
      dev.velocia.spark-version="3.5.9" \
      dev.velocia.gluten-version="1.7.0"
