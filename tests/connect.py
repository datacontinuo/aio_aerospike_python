import unittest


class Connect(unittest.TestCase):
    def test_add_one(self):
        from aio_aerospike_python import AioAerospikeClient
        config = {
            'hosts': [('0.0.0.0', 3000)]
        }
        client = AioAerospikeClient(config)
        self.assertTrue(client.is_connected())


if __name__ == '__main__':
    unittest.main()
