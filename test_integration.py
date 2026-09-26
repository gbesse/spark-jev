import unittest
import importlib.util
import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import MagicMock, patch
from spark_jev import register_jev_if

class SparkTest(unittest.TestCase):
    def test_registers_sql_function(self):
        spark = MagicMock()
        with patch('pyspark.sql.functions.udf', return_value=MagicMock()) as udf:
            self.assertEqual('jev_if', register_jev_if(spark, 'Good?'))
        udf.return_value.asNondeterministic.assert_called_once()
        self.assertEqual('jev_if', spark.udf.register.call_args.args[0])

    @unittest.skipUnless(importlib.util.find_spec('pyspark'), 'PySpark not installed')
    def test_spark_sql_local_with_loopback_jev(self):
        from pyspark.sql import SparkSession
        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                length = int(self.headers['Content-Length'])
                payload = json.loads(self.rfile.read(length))
                content = payload['state']['content']
                score = 0.91 if 'recommend' in content else 0.09
                body = json.dumps({'answers':{'decision':{'type':'noul','noul':score}}}).encode()
                self.send_response(200)
                self.send_header('Content-Type','application/json')
                self.send_header('Content-Length',str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            def log_message(self, *args):
                pass
        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        old_key = os.environ.get('JEV_API_KEY')
        os.environ['JEV_API_KEY'] = 'test-only'
        spark = None
        try:
            spark = SparkSession.builder.master('local[1]').appName('spark-jev-test').config('spark.ui.enabled','false').getOrCreate()
            register_jev_if(spark, 'Does it recommend?', endpoint=f'http://127.0.0.1:{server.server_port}/v1/systemone')
            spark.createDataFrame([('I recommend it',), ('I regret it',)], ['review']).createOrReplaceTempView('reviews')
            routes = {row.route for row in spark.sql('SELECT jev_if(review).route AS route FROM reviews').collect()}
            self.assertEqual({'yes','no'}, routes)
        finally:
            if spark is not None:
                spark.stop()
            server.shutdown()
            if old_key is None:
                os.environ.pop('JEV_API_KEY',None)
            else:
                os.environ['JEV_API_KEY'] = old_key
