import pandas as pd
import numpy as np
from itertools import combinations
from sklearn.model_selection import cross_val_score
from sklearn.feature_selection import RFE, SelectKBest, chi2
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier, AdaBoostClassifier
from sklearn.preprocessing import MinMaxScaler
from sklearn.model_selection import GridSearchCV
from ..constants import *
from ..settings import *


__all__ = ["clean_columns", "mem_to_label", "label_to_mem",
           "generate_classifiers", "get_best_classifier", "get_class_predict",
           "get_accuracy", "get_best_model", "SelectParam", "SelectFeatures"]


def clean_columns(data_set, columns_clean_func):
    """pre process feature columns before training.

    :param data_set: feature data set
    :param columns_clean_func: dictionary with column as key and clean_func as
           value

    """
    for column, clean_func in columns_clean_func.items():
        data_set.update(data_set[column].map(clean_func))
    return data_set


def mem_to_label(value, mem_split):
    """turn mem to label"""
    for index, split in enumerate(mem_split):
        if value <= split:
            return index


def label_to_mem(value, mem_predict):
    """turn label to mem"""
    for index, predict in enumerate(mem_predict):
        if value <= index:
            return predict


def generate_classifiers(mem_split, class_num):
    """Get all the classification criteria combinations for the specified
    number of categories
    """
    return [list(c) + mem_split[-1:] for c in combinations(mem_split[:-1],
                                                           class_num-1)]


def get_best_classifier(data_set, split_group, check_ratio=True):
    """get best class boundary criteria"""
    best_score = 0
    model = RandomForestClassifier()
    best_classifier = None
    for split in split_group:
        temp_set = data_set.copy()
        temp_set[LABEL] = temp_set[FeatureColumns.USE_MEM].apply(
            mem_to_label, args=(split,))

        # according to sklearn, each class at least three sample
        label_counts = temp_set[LABEL].value_counts()
        if label_counts.min() < 3:
            continue

        # Check whether the smallest category take up [0.001,0.01] of the total
        # based on past experience, the classification of such cases better
        if check_ratio:
            if not 0.001 < label_counts.min() / label_counts.sum() < 0.1:
                continue
        score = cross_val_score(model, temp_set[FeatureColumns.FEATURES],
                                temp_set[LABEL]).mean()
        if best_score < score:
            best_score = score
            best_classifier = split
    if not best_classifier:
        return get_best_classifier(data_set, split_group, check_ratio=False)
    return best_classifier


def get_class_predict(class_split, ratio):
    """get predict memory for each class

    :param class_split: use class split boundary to get class predict
    :param ratio: ratio for two adjacent class split boundary to generate
           the predict value. for example, class_split=[300, 800, 1500, 5000],
           ratio=0.5, then class_predict[0] = class_split[0]*0.5 +
           class_split[1]*0.5 and class_predict[-1] = class_split[-1], then
           class_predict = [550, 1150, 3250, 5000]
    """
    class_predict = []
    for i in range(len(class_split) - 1):
        class_predict.append(ratio*class_split[i] + (1-ratio)*class_split[i+1])
    class_predict.append(class_split[-1])
    return class_predict


def get_accuracy(eval_set):
    """get eval_set's total count, predict error count and accuracy, if total
    count equal zero, then accuracy equal one.
    """
    count = np.shape(eval_set)[0]
    if not count:
        return 0, 0, 1
    err_set = eval_set[eval_set[FeatureColumns.USE_MEM] >
                       eval_set[PREDICT_MEM]]
    err_count = np.shape(err_set)[0]
    accuracy = 1 - err_count / count
    return count, err_count, accuracy


def get_best_model(X ,y):
    """Select best model from RandomForestClassifier and AdaBoostClassifier"""
    ensembles = [
        (RandomForestClassifier, SelectParam({
            'estimator': RandomForestClassifier(warm_start=True, random_state=7),
            'param_grid': {
                'n_estimators': [10, 15, 20],
                'criterion': ['gini', 'entropy'],
                'max_features': [FEATURE_NUM+n for n in [-4, -2, 0]],
                'max_depth': [10, 15],
                'bootstrap': [True],
                'warm_start': [True],
            },
            'n_jobs':1
         })),
        (AdaBoostClassifier, SelectParam({
            'estimator': AdaBoostClassifier(random_state=7),
            'param_grid': {
                'algorithm': ['SAMME', 'SAMME.R'],
                'n_estimators': [10, 15, 20],
                'learning_rate': [1e-3, 1e-2, 1e-1]
            },
            'n_jobs': 1
        }))
    ]

    best_score = 0
    best_model = None
    for ensemble, select in ensembles:
        param = select.get_param(X, y)
        model = ensemble(**param)
        score = cross_val_score(model, X, y).mean()
        if best_score < score:
            best_score = score
            best_model = model
    return best_model


class SelectFeatures:
    @staticmethod
    def random_forest(X, y, num=10):
        """RandomForestClassifier method to choose features"""
        model = RandomForestClassifier()
        model.fit(X, y)
        feature_imp = pd.DataFrame(model.feature_importances_, index=X.columns,
                                   columns=["importance"])
        return feature_imp.sort_values("importance", ascending=False).\
            head(num).index.tolist()

    @staticmethod
    def k_best(X, y, num=10):
        """Kbest method to choose features"""
        X_minmax = MinMaxScaler(feature_range=(0, 1)).fit_transform(X)
        X_scored = SelectKBest(score_func=chi2, k='all').fit(X_minmax, y)
        feature_scoring = pd.DataFrame({
                'feature': X.columns,
                'score': X_scored.scores_
            })
        return feature_scoring.sort_values('score', ascending=False).\
            head(num)['feature'].tolist()

    @staticmethod
    def rfe(X, y, num=10):
        """RFE method to choose features"""
        rfe = RFE(LogisticRegression(), num)
        rfe.fit(X, y)
        feature_rfe_scoring = pd.DataFrame({
                'feature': X.columns,
                'score': rfe.ranking_
            })
        return feature_rfe_scoring[feature_rfe_scoring['score'] == 1]['feature'].tolist()


class SelectParam:
    def __init__(self, params):
        self.params = params

    def get_param(self, X, y):
        grid = GridSearchCV(**self.params)
        grid.fit(X, y)
        return grid.best_params_

