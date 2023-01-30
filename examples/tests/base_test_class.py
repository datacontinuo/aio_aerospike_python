import unittest
from aio_aerospike_python import AioAerospikeClient
import logging
logger = logging.getLogger("SomeTest.testSomething")
import sys
import asyncio
events = []
class BaseTestClass(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        config = {
            'hosts': [('0.0.0.0', 3000)]
        }
        cls.client = AioAerospikeClient(config)
        cls.assertTrue(cls.client.is_connected())
        logger.error("co")

    async def get_key(self):
        key = ("test","test",1)
        data = await self.client.get(key)
        print(self.client)
        print(data)
        self.assertEqual(type(self.client), AioAerospikeClient )
        logger.error(data)

if __name__ == '__main__':
    logging.basicConfig( stream=sys.stderr )
    logging.getLogger( "SomeTest.testSomething" ).setLevel( logging.DEBUG )    
    unittest.main()
