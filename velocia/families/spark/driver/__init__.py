"""Velocia's driver program for Apache Spark (docs/06-design.md §5.1-5.3).

Runs inside the engine image on its Python (3.10 in the official Apache Spark 3.5 images), so
this package uses only PySpark and the standard library, imports its own modules relatively and
never imports the rest of ``velocia``. It is shipped at submit time as a zip (``--py-files``)
and imported there as ``velocia_driver``.
"""
