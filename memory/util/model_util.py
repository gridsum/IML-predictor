from sklearn.externals import joblib
import os
import logging
from ..model.settings import MODEL_DIR


def load_model(model_path):
    logging.info("loading model %s" % model_path)
    return joblib.load(model_path)


class ModelFactory:
    @staticmethod
    def get_model(model_name):
        model_path = os.path.join(MODEL_DIR, model_name)
        return load_model(model_path)

    @staticmethod
    def prepare_model():
        model_dict = {}
        for model_name in os.listdir(MODEL_DIR):
            model_dict[model_name] = ModelFactory.get_model(model_name)
        return model_dict