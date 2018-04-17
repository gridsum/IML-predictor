import unittest
from .constants import UrlConstants
from .common import ToolBoxAPIWrapper
from ..error import ErrorCode

query_id = '7c4b6676d72f4917:8f411dac00000000'
SQL = "select statement from impala_query_info where query_id=" + repr(query_id)
DATABASE = "dataengineering"
POOL_DEFAULT = "default"
POOL_HADOOP_AD = "root.hadoop-ad"


class MemoryPredictTest(unittest.TestCase):
    def test_predict_should_not_ok_when_no_sql(self):
        params = {
            'db': DATABASE,
            'pool': POOL_DEFAULT
        }
        status_code, json_data = ToolBoxAPIWrapper.query_memory(
            UrlConstants.MEMORY_PREDICT_TEMPLATE, params)
        self.assertTrue(status_code == 400)
        self.assertTrue(json_data['error_code'] == ErrorCode.PARAMETER_ERROR)

    def test_predict_should_not_ok_when_sql_is_empty(self):
        params = {
            'sql': '',
            'db': DATABASE,
            'pool': POOL_DEFAULT
        }
        status_code, json_data = ToolBoxAPIWrapper.query_memory(
            UrlConstants.MEMORY_PREDICT_TEMPLATE, params)
        self.assertTrue(status_code == 400)
        self.assertTrue(json_data['error_code'] == ErrorCode.PARAMETER_ERROR)

    def test_predict_should_not_ok_when_no_db(self):
        params = {
            'sql': SQL,
            'pool': POOL_DEFAULT
        }
        status_code, json_data = ToolBoxAPIWrapper.query_memory(
            UrlConstants.MEMORY_PREDICT_TEMPLATE, params)
        self.assertTrue(status_code == 400)
        self.assertTrue(json_data['error_code'] == ErrorCode.PARAMETER_ERROR)

    def test_predict_should_not_ok_when_db_is_empty(self):
        params = {
            'sql': SQL,
            'db': '',
            'pool': POOL_DEFAULT
        }
        status_code, json_data = ToolBoxAPIWrapper.query_memory(
            UrlConstants.MEMORY_PREDICT_TEMPLATE, params)
        self.assertTrue(status_code == 400)
        self.assertTrue(json_data['error_code'] == ErrorCode.PARAMETER_ERROR)

    def test_predict_should_ok_when_pool_is_empty(self):
        params = {
            'sql': SQL,
            'db': DATABASE,
        }
        status_code, json_data = ToolBoxAPIWrapper.query_memory(
            UrlConstants.MEMORY_PREDICT_TEMPLATE, params)
        self.assertTrue(status_code == 200)
        self.assertTrue(json_data['error_code'] == ErrorCode.SUCCESS)

    def test_predict_should_ok_when_pool_is_hadoop_ad(self):
        params = {
            'sql': SQL,
            'db': DATABASE,
            'pool': POOL_HADOOP_AD
        }
        status_code, json_data = ToolBoxAPIWrapper.query_memory(
            UrlConstants.MEMORY_PREDICT_TEMPLATE, params)
        self.assertTrue(status_code == 200)
        self.assertTrue(json_data['error_code'] == ErrorCode.SUCCESS)

if __name__ == "__main__":
    unittest.main()

