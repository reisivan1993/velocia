# Vanilla Apache Spark engine image for Velocia.
# The Velocia driver program is shipped at submit time (ADR-3), so this image holds only Spark.
# Base: official Docker image spark:3.5.9-scala2.12-java17-python3-ubuntu, pinned by digest.
FROM spark@sha256:d40624d060dcb1ecc5b19311f4ec121b3a81d8f797950eaacd27f0585053a4b9

LABEL org.opencontainers.image.title="velocia-engine-spark-vanilla" \
      org.opencontainers.image.description="Apache Spark 3.5.9 (Scala 2.12, Java 17, Python 3) for Velocia" \
      org.opencontainers.image.licenses="Apache-2.0" \
      dev.velocia.engine="spark-vanilla" \
      dev.velocia.spark-version="3.5.9"
