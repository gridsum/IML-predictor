from .model.util import label_to_mem
from .model.settings import MODEL_GROUP


def get_model_name(pool):
    """get model suits specified pool.
    :param pool: pool name, specified by post params

    """
    for g in MODEL_GROUP:
        if pool == g.get("pool_group"):
            return g['name']
    for g in MODEL_GROUP:
        if not g.get("pool_group"):
            return g['name']


def predict(m, features):
    label = m.clf.predict([[features.get(f) for f in m.features]])
    mem = label_to_mem(label, m.class_predict)
    return int(mem)
