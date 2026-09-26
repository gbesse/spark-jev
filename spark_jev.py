"""Register a Spark SQL UDF returning typed Jev decision fields."""
from jev_common import JevClient

def register_jev_if(spark, question, *, threshold=0.8, name='jev_if',
                    endpoint='https://api.typesafe.ai/v1/systemone',
                    max_calls_per_worker=10000, cache_size=1024):
    from pyspark.sql.functions import udf
    from pyspark.sql.types import DoubleType, StringType, StructField, StructType
    schema = StructType([StructField('route', StringType(), False),
                         StructField('probability', DoubleType(), True),
                         StructField('state_sha256', StringType(), True)])
    def judge(text):
        if not hasattr(judge, '_client'):
            judge._client = JevClient(question, threshold=threshold, endpoint=endpoint,
                                      max_calls=max_calls_per_worker, cache_size=cache_size)
        result = judge._client.decide(text)
        return result['route'], result['probability'], result['state_sha256']
    spark.udf.register(name, udf(judge, schema).asNondeterministic())
    return name
