import unittest
import aerospike
import asyncio
from aio_aerospike_python import AioAerospikeClient


from aerospike_helpers.operations import operations
from aerospike import predicates


def get_base_class():
    import sys
    if "3.7" in sys.version:
        return unittest.TestCase
    else:
        return unittest.IsolatedAsyncioTestCase


class ScanTest(get_base_class()):
    @classmethod
    def setUpClass(cls) -> None:
        config = {
            'hosts': [('0.0.0.0', 3000)]
        }
        cls._client = AioAerospikeClient(config)
        cls._scan = cls._client.scan("test", "test")


    # async def test_execute_background(self):
    #     ops = [
    #         operations.increment("a", 100)
    #     ]
    #     self._scan.add_ops(ops)
    #     await self._scan.execute_background()

    def test_paginate(self):
        return self._scan.paginate()


    def test_select(self):
        return self._scan.select("a")

