"""Planted faults for Velocia's tests: wraps SparkSession.sql once pyspark is imported.

spark.velocia.test.fault = wrong  -> every query result is cut to one row (wrong results)
                         = noisy  -> each query first sleeps 1.5 s x its repetition number (high
                                     variation, independent of how fast the machine is)
                         = slow   -> each query first runs a Spark job far longer than any timeout
                         = unsorted -> DataFrame.orderBy does nothing (unsorted job output)
"""

import importlib.abc
import importlib.util
import sys
import time

TARGET = "pyspark.sql.session"


def _rep(session):
    """The repetition from Velocia's job group tag velocia:<run>:<unit>:<rep>; 0 if none."""
    group = session.sparkContext.getLocalProperty("spark.jobGroup.id") or ""
    try:
        return max(int(group.rsplit(":", 1)[1]), 0)
    except (IndexError, ValueError):
        return 0


def _patch(module):
    original = module.SparkSession.sql

    def sql(self, *args, **kwargs):
        fault = self.conf.get("spark.velocia.test.fault", "")
        if fault == "noisy":
            time.sleep(1.5 * _rep(self))
        elif fault == "slow":
            self.range(10**13).selectExpr("sum(id * id)").collect()
        df = original(self, *args, **kwargs)
        return df.limit(1) if fault == "wrong" else df

    module.SparkSession.sql = sql

    from pyspark.sql.dataframe import DataFrame  # imported by session.py already

    original_order_by = DataFrame.orderBy

    def order_by(self, *cols, **kwargs):
        if self.sparkSession.conf.get("spark.velocia.test.fault", "") == "unsorted":
            return self
        return original_order_by(self, *cols, **kwargs)

    DataFrame.orderBy = order_by
    DataFrame.sort = order_by


class _Hook(importlib.abc.MetaPathFinder):
    def find_spec(self, name, path, target=None):
        if name != TARGET:
            return None
        sys.meta_path.remove(self)
        spec = importlib.util.find_spec(name)
        loader = spec.loader
        exec_module = loader.exec_module  # shared by every module in pyspark.zip

        def exec_and_patch(module):
            if module.__name__ != TARGET:
                return exec_module(module)
            del loader.exec_module  # back to the class method for later modules
            exec_module(module)
            _patch(module)

        loader.exec_module = exec_and_patch
        return spec


sys.meta_path.insert(0, _Hook())
