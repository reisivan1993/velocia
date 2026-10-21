"""Planted faults for Velocia's tests: wraps SparkSession.sql once pyspark is imported.

spark.velocia.test.fault = wrong  -> every query result is cut to one row (wrong results)
                         = noisy  -> each query first sleeps a random 0-1.5 s (high variation)
                         = slow   -> each query first runs a Spark job far longer than any timeout
"""

import importlib.abc
import importlib.util
import random
import sys
import time

TARGET = "pyspark.sql.session"


def _patch(module):
    original = module.SparkSession.sql

    def sql(self, *args, **kwargs):
        fault = self.conf.get("spark.velocia.test.fault", "")
        if fault == "noisy":
            time.sleep(random.uniform(0.0, 1.5))
        elif fault == "slow":
            self.range(10**13).selectExpr("sum(id * id)").collect()
        df = original(self, *args, **kwargs)
        return df.limit(1) if fault == "wrong" else df

    module.SparkSession.sql = sql


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
