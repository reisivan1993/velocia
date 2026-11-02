"""Files on any storage the cluster can reach (``file:``, ``s3a:``, ``hdfs:``, ...).

Uses the Hadoop FileSystem API inside the Spark JVM, so the cluster's own filesystem support
and credentials apply and Velocia ships no storage client (external runner, docs/03-spec.md §4).
Works on plain local paths too.
"""

from typing import Any


class Fs:
    """Hadoop FileSystem calls through Py4J. Keep one per SparkContext; it outlives ``stop()``."""

    def __init__(self, spark: Any) -> None:
        sc = spark.sparkContext
        self._jvm = sc._jvm
        self._conf = sc._jsc.hadoopConfiguration()

    def _path(self, uri: str) -> Any:
        return self._jvm.org.apache.hadoop.fs.Path(uri)

    def _fs(self, uri: str) -> Any:
        return self._path(uri).getFileSystem(self._conf)

    def exists(self, uri: str) -> bool:
        return bool(self._fs(uri).exists(self._path(uri)))

    def list(self, uri: str) -> list[tuple[str, int, bool]]:
        """``(name, bytes, is_dir)`` of the entries directly in ``uri``, sorted by name."""
        entries = self._fs(uri).listStatus(self._path(uri))
        return sorted(
            (e.getPath().getName(), int(e.getLen()), bool(e.isDirectory())) for e in entries
        )

    def size(self, uri: str) -> int | None:
        if not self.exists(uri):
            return None
        return int(self._fs(uri).getFileStatus(self._path(uri)).getLen())

    def read_text(self, uri: str) -> str | None:
        if not self.exists(uri):
            return None
        stream = self._fs(uri).open(self._path(uri))
        try:
            reader = self._jvm.java.io.BufferedReader(
                self._jvm.java.io.InputStreamReader(stream, "UTF-8")
            )
            lines = []
            line = reader.readLine()
            while line is not None:
                lines.append(line)
                line = reader.readLine()
            return "\n".join(lines)
        finally:
            stream.close()

    def write_text(self, uri: str, text: str) -> None:
        stream = self._fs(uri).create(self._path(uri), True)
        try:
            stream.write(bytearray(text.encode("utf-8")))
        finally:
            stream.close()

    def _local(self) -> Any:
        # The raw local filesystem: no .crc side files next to downloaded logs.
        return self._jvm.org.apache.hadoop.fs.FileSystem.getLocal(self._conf).getRaw()

    def _copy(self, src_fs: Any, src: str, dst_fs: Any, dst: str) -> None:
        # Stream copy, not copyFromLocalFile: s3a's version fails on single files
        # ("Cannot get relative path for URI").
        ok = self._jvm.org.apache.hadoop.fs.FileUtil.copy(
            src_fs, self._path(src), dst_fs, self._path(dst), False, True, self._conf
        )
        if not ok:
            raise OSError(f"copy {src} -> {dst} failed")

    def upload(self, local: str, uri: str) -> None:
        self._copy(self._local(), local, self._fs(uri), uri)

    def download(self, uri: str, local: str) -> None:
        self._copy(self._fs(uri), uri, self._local(), local)

    def delete(self, uri: str) -> None:
        self._fs(uri).delete(self._path(uri), True)


def join(base: str, *parts: str) -> str:
    return "/".join([base.rstrip("/"), *(p.strip("/") for p in parts)])
