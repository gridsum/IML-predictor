from hdfs.ext.kerberos import KerberosClient
from hdfs.client import Client
from krbcontext import krbcontext
import shutil
import os
import logging
import subprocess

from ..settings import (KEYTAB_PATH, PRINCIPAL, HDFS, SparkSubmit,
                        IMPALA_VERSION, NEED_CERTIFICATE)


__all__ = ["HDFSClient", "SparkClient"]


class HDFSClient:
    """using Kerberos authentication for downloading feature data"""
    @staticmethod
    def generate_temp_files(need_certificate=NEED_CERTIFICATE):
        if need_certificate:
            with krbcontext(using_keytab=True, keytab_file=KEYTAB_PATH,
                            principal=PRINCIPAL):
                for node in HDFS.NODES:
                    try:
                        hdfs_client = KerberosClient(node)
                        hdfs_client.download(HDFS.REMOTE_PATH, HDFS.LOCAL_PATH,
                                             n_threads=HDFS.THREAD_NUM)
                    except Exception as err:
                        logging.info(err)
                    else:
                        return
                logging.error("Failed to download remote HDFS file.")
                raise Exception("Failed to download remote HDFS file.")
        else:
            for node in HDFS.NODES:
                try:
                    hdfs_client = Client(node)
                    hdfs_client.download(HDFS.REMOTE_PATH, HDFS.LOCAL_PATH,
                                         n_threads=HDFS.THREAD_NUM)
                except Exception as err:
                    logging.info(err)
                else:
                    return
            logging.error("Failed to download remote HDFS file.")
            raise Exception("Failed to download remote HDFS file.")

    @staticmethod
    def remove_temp_files():
        if os.path.exists(HDFS.LOCAL_PATH):
            shutil.rmtree(HDFS.LOCAL_PATH)


class SparkClient:
    """Submit Spark Task.

    :param start_day: feature data start day.
    :param end_day: feature data end day.

    """
    def __init__(self, start_day, end_day):
        self.start_day = start_day
        self.end_day = end_day

    def generate_cmd(self):
        cmd = [SparkSubmit.PREFIX]
        for key, value in SparkSubmit.SPARK_SUBMIT_PARAMS.items():
            cmd.append("--%s %s" % (key, value))
        cmd.extend([SparkSubmit.APP, str(self.start_day), str(self.end_day),
                    IMPALA_VERSION, HDFS.REMOTE_PATH])
        return cmd

    def run(self):
        cmd = self.generate_cmd()
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                             stderr=subprocess.DEVNULL)
        for line in p.stdout.readlines():
            if "Finished Features Task" in line:
                break








