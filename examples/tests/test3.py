import unittest
import asyncio
from aio_aerospike_python import AioAerospikeClient

class Connect(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.l = [1,2,3]
        config = {
            'hosts': [('0.0.0.0', 3000)]
        }
        cls.client = AioAerospikeClient(config)
        # cls.assertTrue(cls.client.is_connected())
    async def test_put(self):
        key = ("test","test",1)
        bins = {"1":1}
        await self.client.put(key, bins)
        # print(self.client)
        # print(data)


    async def test_get(self):
        key = ("test","test",1)
        data = await self.client.get(key)
        print(data)

    def test1(self):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.test_put())
        loop.run_until_complete(self.test_get())

    def test2(self):
        self.l.append(6)
        print(self.l)
    def test4(self):
        self.l.append(7)
        print(self.l)

if __name__ == '__main__':
    unittest.main()
