from memory.model.settings import SparkSubmit, HDFS, IMPALA_VERSION

cmd = [SparkSubmit.PREFIX]

for key, value in SparkSubmit.SPARK_SUBMIT_PARAMS.items():
    cmd.append("--%s %s" % (key, value))

cmd.extend([SparkSubmit.APP, "$1", "$2", IMPALA_VERSION, HDFS.REMOTE_PATH])

with open("spark.sh", "w") as f:
    f.write(' '.join(cmd))

