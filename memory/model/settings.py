import os
from ..settings import (KEYTAB_PATH, PRINCIPAL, LOGGING,
                        ImpalaConstants, NEED_CERTIFICATE)

__all__ = ["KEYTAB_PATH", "PRINCIPAL", "NEED_CERTIFICATE", "LOGGING",
           "MODEL_DIR", "FEATURE_FILE", "RESULT_FILE", "HDFS", "SparkSubmit",
           "IMPALA_VERSION", "FEATURE_NUM", "COLUMNS_CLEAN_FUNC",
           "MEMORY_SPLIT", "ACCURACY_SPLIT", "CLASS_NUM",
           "CROSS_VALIDATE_RATIO", "MEMORY_PREDICT_RATIO", "MODEL_GROUP"]

BASE_PATH = os.path.dirname(__file__)
MODEL_DIR = os.path.join(BASE_PATH, "model_dir")
FEATURE_FILE = os.path.join(BASE_PATH, "feature.csv")
RESULT_FILE = os.path.join(BASE_PATH, "result.csv")
IMPALA_VERSION = ImpalaConstants.VERSION


class HDFS:
    FILE_PATTERN = "part-*"
    THREAD_NUM = 10
    LOCAL_PATH = os.path.join(BASE_PATH, "temp_dir")
    REMOTE_PATH = "/user/dataengineering/iml-predictor/feature/"
    NODES = ['http://gs-server-1046:50070', 'http://gs-server-1047:50070']


class SparkSubmit:
    LOCAL = False
    PREFIX = "spark-submit"
    APP = "feature-engineering.jar"
    SPARK_SUBMIT_PARAMS = {
        'class': "com.gridsum.de.impala.Features",
        'master': 'yarn',
        'driver-memory': "10g",
        'executor-memory': "20g",
        "num-executors": 20,
        "executor-cores": 3,
    }

FEATURE_NUM = 12
COLUMNS_CLEAN_FUNC = {'mSize': round, 'mFiles': round, 'useMemMB': round}
MEMORY_SPLIT = [50, 200, 300, 500, 800, 1500, 5000]
ACCURACY_SPLIT = [500, 1000, 2000, 5000]
CLASS_NUM = 4
CROSS_VALIDATE_RATIO = 0.67
MEMORY_PREDICT_RATIO = 0.5
MODEL_GROUP = [
    {
        'name': "first",
        'pool_group': ["root.hadoop-ad", "root.hadoop-tvd", "root.hadoop-am",
                       'root.app-ad']
    },
    {
        'name': "second",
        'pool_group': ['root.app-gwd', 'root.hadoop-wd']
    },
    {
        'name': "third",
        'pool_group': ["root.consultant", "root.dm-pool"]
    },
    {
        'name': "fourth",
        'pool_group': ["root.datascience", "root.default", "root.gdp-personal"]
    },
    {
        'name': "fifth",
    }
]