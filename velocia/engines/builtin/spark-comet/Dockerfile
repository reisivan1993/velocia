# Apache DataFusion Comet engine image for Velocia: vanilla Spark 3.5.9 plus the Comet jar.
# The jar is fetched from Maven Central and verified by checksum; it bundles native libraries
# for linux/amd64 and linux/aarch64.
FROM spark@sha256:d40624d060dcb1ecc5b19311f4ec121b3a81d8f797950eaacd27f0585053a4b9

ARG COMET_VERSION=1.1.0
ADD --chmod=644 \
    --checksum=sha256:44ed79e91810aeb4c093f3449abb06ce1f6f29fa0f942420ac4106c7c9dd2fca \
    https://repo1.maven.org/maven2/org/apache/datafusion/comet-spark-spark3.5_2.12/${COMET_VERSION}/comet-spark-spark3.5_2.12-${COMET_VERSION}.jar \
    /opt/spark/jars/

LABEL org.opencontainers.image.title="velocia-engine-spark-comet" \
      org.opencontainers.image.description="Apache Spark 3.5.9 with Apache DataFusion Comet 1.1.0 for Velocia" \
      org.opencontainers.image.licenses="Apache-2.0" \
      dev.velocia.engine="spark-comet" \
      dev.velocia.spark-version="3.5.9" \
      dev.velocia.comet-version="1.1.0"
