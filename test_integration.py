import unittest
from unittest.mock import MagicMock, patch
from spark_jev import register_jev_if

class SparkTest(unittest.TestCase):
    def test_registers_sql_function(self):
        spark = MagicMock()
        with patch('pyspark.sql.functions.udf', side_effect=lambda fn, schema: fn):
            self.assertEqual('jev_if', register_jev_if(spark, 'Good?'))
        self.assertEqual('jev_if', spark.udf.register.call_args.args[0])
