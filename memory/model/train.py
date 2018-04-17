import os

import pandas as pd
from sklearn.externals import joblib

from .util import (clean_columns, mem_to_label, label_to_mem,
                   generate_classifiers, get_best_classifier, SelectFeatures,
                   get_class_predict, get_accuracy, get_best_model )
from .error import ModelError
from .settings import *
from .constants import *


class Model:
    """For each model group in constants, create a Model object for saving its
    pool group, model file path, etc.

    :param name: Model object name
    :param pool_group: Which pool group this model belong to

    """
    def __init__(self, name, pool_group=None):
        self.name = name
        self.pool_group = pool_group
        self.path = os.path.join(MODEL_DIR, name)
        self.class_split = None
        self.class_predict = None
        self.features = None
        self.clf = None

    def train(self, data_set):
        if self.pool_group:
            group_set = data_set[data_set[FeatureColumns.POOL].isin(
                self.pool_group)].copy()
        else:
            group_set = data_set.copy()

        if group_set.empty:
            raise ModelError("No train data for %s group" % self.name)

        # remove samples whose use_mem exceed memory limit
        group_set = group_set[group_set[FeatureColumns.USE_MEM] <=
                              MEMORY_SPLIT[-1]]
        # clean feature columns
        group_set = clean_columns(group_set, COLUMNS_CLEAN_FUNC)

        # get all possible classification criteria group
        class_split_group = generate_classifiers(MEMORY_SPLIT,
                                                 CLASS_NUM)

        # get best classification criteria from cross_val_score
        self.class_split = get_best_classifier(group_set, class_split_group)

        # for each class, get its predict use_mem
        self.class_predict = get_class_predict(self.class_split,
                                               MEMORY_PREDICT_RATIO)

        # set label for each sample by its use_mem and class split criteria
        group_set[LABEL] = group_set[FeatureColumns.USE_MEM].apply(
            mem_to_label, args=(self.class_split,))

        # select certain number of features by RandomForestClassifier
        self.features = SelectFeatures.random_forest(
            group_set[FeatureColumns.FEATURES],
            group_set[LABEL],
            FEATURE_NUM)

        # get best classifier
        self.clf = get_best_model(group_set[self.features],
                                  group_set[LABEL])

        self.clf.fit(group_set[self.features], group_set[LABEL])

        # create model dir if not exist
        if not os.path.exists(MODEL_DIR):
            os.mkdir(MODEL_DIR)
        joblib.dump(self, self.path + ".tmp")

    def validate(self, data_set):
        if self.pool_group:
            group_set = data_set[data_set[FeatureColumns.POOL].isin(
                self.pool_group)].copy()
        else:
            other_list = []
            for g in MODEL_GROUP:
                if g.get('pool_group'):
                    other_list.extend(g['pool_group'])
            group_set = data_set[~data_set['pool'].isin(other_list)].copy()

        if group_set.empty:
            return

        # remove samples whose use_mem exceed memory limit
        group_set = group_set[group_set[FeatureColumns.USE_MEM] <=
                              MEMORY_SPLIT[-1]]

        # clean feature columns
        group_set = clean_columns(group_set, COLUMNS_CLEAN_FUNC)

        # get label by corresponding model
        group_set[LABEL] = self.clf.predict(group_set[self.features])

        # get predict memory limit by label and class_predict
        group_set[PREDICT_MEM] = group_set[LABEL].apply(
            label_to_mem, args=(self.class_predict,))

        # get validate result report
        return self.get_result(group_set)

    def get_result(self, eval_set):
        use_mem_total = eval_set[FeatureColumns.USE_MEM].sum() // 1024
        predict_mem_total = eval_set[PREDICT_MEM].sum() // 1024
        predict_to_use_ratio = predict_mem_total / use_mem_total
        total_count, total_err_count, total_accuracy = get_accuracy(
            eval_set)

        # get certain accuracy for specified criteria in constants file, for
        # example, ACCURACY_SPLIT=[500, 1000, 2000, 5000], then we
        # calculate accuracy for use_mem less than 500, 1000, 2000, 5000
        split_accuracy = []
        for i in range(len(ACCURACY_SPLIT)):
            if i == 0:
                split_set = eval_set[eval_set[FeatureColumns.USE_MEM] <=
                                     ACCURACY_SPLIT[i]]
            else:
                split_set = eval_set[(eval_set[FeatureColumns.USE_MEM] <=
                                      ACCURACY_SPLIT[i]) &
                                     (eval_set[FeatureColumns.USE_MEM] >
                                      ACCURACY_SPLIT[i-1])]
            split_accuracy.append(get_accuracy(split_set)[2])

        return pd.DataFrame([{
            'pool': repr(self.pool_group),
            'split': self.class_split,
            'predict': self.class_predict,
            'useMemGB': use_mem_total,
            'predictMemGB': predict_mem_total,
            'predict2useRatio': predict_to_use_ratio,
            'queryCnt': total_count,
            'predictErrCnt': total_err_count,
            'accuracy': total_accuracy,
            'accuracy_%s' % ACCURACY_SPLIT[0]: split_accuracy[0],
            'accuracy_%s_%s' % (ACCURACY_SPLIT[0],
                                ACCURACY_SPLIT[1]): split_accuracy[1]
            ,
            'accuracy_%s_%s' % (ACCURACY_SPLIT[1],
                                ACCURACY_SPLIT[2]): split_accuracy[2]
            ,
            'accuracy_%s_%s' % (ACCURACY_SPLIT[2],
                                ACCURACY_SPLIT[3]): split_accuracy[3]
        }], columns=['pool', 'split', 'predict','queryCnt',
                     'predictErrCnt', 'accuracy', 'useMemGB', 'predictMemGB',
                     'predict2useRatio', 'accuracy_%s' % ACCURACY_SPLIT[0],
                     'accuracy_%s_%s' % (ACCURACY_SPLIT[0],
                                         ACCURACY_SPLIT[1]),
                     'accuracy_%s_%s' % (ACCURACY_SPLIT[1],
                                         ACCURACY_SPLIT[2]),
                     'accuracy_%s_%s' % (ACCURACY_SPLIT[2],
                                         ACCURACY_SPLIT[3])
                     ])













