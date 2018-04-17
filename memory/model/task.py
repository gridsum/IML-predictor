import glob
import logging
import os
import pandas as pd
from sklearn.externals import joblib
from datetime import datetime

from .util import HDFSClient, SparkClient
from .constants import DATE_FORMAT
from .settings import *
from .train import Model
from .error import ModelError


def train(train_set):
    for group in MODEL_GROUP:
        m = Model(**group)
        m.train(train_set)


def validate(validate_set):
    total_result = []
    for group in MODEL_GROUP:
        m = joblib.load(os.path.join(MODEL_DIR, group['name']) + ".tmp")
        result = m.validate(validate_set)
        if result is not None:
            total_result.append(result)
    with open(RESULT_FILE, 'a') as r:
        pd.concat(total_result).to_csv(r, index=False)


class Task:
    """Task for downloading feature data, generating cross-validating report
    and generating model.

    :param start_day: downloading data start day, first parameter of spark task
    :param end_day: downloading data end day, second parameter of spark task

    """
    def __init__(self, start_day, end_day):
        self.start_day = int(start_day)
        self.end_day = int(end_day)
        if self.start_day > self.end_day:
            raise ModelError("start_day: %s larger than end_day: %s",
                             self.start_day, self.end_day)

    def generate_feature_file(self):
        """start spark task for generating feature data and download result
        from remote hdfs server.
        """
        logging.info("Starting generating feature file")
        if SparkSubmit.LOCAL:
            SparkClient(self.start_day, self.end_day).run()
        HDFSClient.remove_temp_files()
        HDFSClient.generate_temp_files()
        logging.info("Saving features data to %s" % FEATURE_FILE)
        with open(FEATURE_FILE, 'w') as f:
            pd.concat(map(pd.read_csv, glob.glob(os.path.join(
                HDFS.LOCAL_PATH, HDFS.FILE_PATTERN)))).to_csv(f, index=False)

    def run(self, generate_feature=False, cross_validate=True):
        """Start task by parameters from users

        :param generate_feature: Do not generate feature data if False, to
               avoid duplicate download file
        :param cross_validate: Do not cross-validate if False, to save time

        """
        if generate_feature:
            self.generate_feature_file()

        # check feature file exists
        if not os.path.exists(FEATURE_FILE):
            raise ModelError("Feature file: %s not exists", FEATURE_FILE)

        # check feature file not empty
        data_set = pd.read_csv(FEATURE_FILE)
        if data_set.empty:
            raise ModelError("%s contains no data", FEATURE_FILE)

        # filter feature data from start_day to end_day if use old feature file
        if not generate_feature:
            data_set = data_set[(data_set['day'] >= self.start_day) & (
                data_set['day'] <= self.end_day)]

            # check feature data not empty
            if data_set.empty:
                raise ModelError("No Feature Data from %s to %s",
                                 self.start_day, self.end_day)

        # if need cross_validate
        if cross_validate:
            train_end = self.get_train_end(CROSS_VALIDATE_RATIO)
            train_set = data_set[data_set['day'] <= train_end]
            validate_set = data_set[data_set['day'] > train_end]
            if train_set.empty or validate_set.empty:
                raise ModelError("train_set or validate_set empty")
            logging.info("Start cross-validating, Train day:%s~%s, "
                         "Validate day:%s~%s" % (self.start_day, train_end,
                                                 train_end+1, self.end_day))
            train(train_set)
            logging.info("Saving cross-validate result to %s" % RESULT_FILE)
            with open(RESULT_FILE, 'w') as r:
                r.write("trainDay,validateDay\n%s~%s,%s~%s\n" %
                        (self.start_day, train_end, train_end+1, self.end_day))
            validate(validate_set)

        # train model
        logging.info("Start training model")
        train(data_set)
        logging.info("Succeed training model")

    def get_train_end(self, ratio=0.67):
        """get train set and validate set split datetime for cross-validating,
        for example, if self.start_day='20180101', self.end_day='20180105',
        then duration=timedelta(4), step=timedelta(2, 58752), then return
        20180103
        """
        fmt = DATE_FORMAT
        start_day = datetime.strptime(str(self.start_day), fmt)
        end_day = datetime.strptime(str(self.end_day), fmt)
        duration = end_day - start_day
        step = duration * ratio
        # if train_end equal to start_day
        if step.days < 1:
            raise ModelError(
                "Could not get split datetime for cross-validating when "
                "start_day: %s, end_day: %s, ratio: %s",
                self.start_day, self.end_day, ratio)
        train_end = start_day + step
        return int(train_end.strftime(fmt))