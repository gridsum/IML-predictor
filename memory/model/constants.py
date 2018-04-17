from ..constants import DATE_FORMAT

__all__ = ["DATE_FORMAT", "FeatureColumns", "LABEL", "PREDICT_MEM"]


class FeatureColumns:
    POOL = "pool"
    USE_MEM = "useMemMB"
    FEATURES = ['mLayer', 'mSize', 'mFiles', 'events', 'agg', 'exg', 'alt',
                'select', 'hjoin', 'ljoin', 'scan', 'sort', 'union', 'top']

LABEL = "label"
PREDICT_MEM = "predictMemMB"

