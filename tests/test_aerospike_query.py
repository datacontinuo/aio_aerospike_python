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


class QueryTest(get_base_class()):
    @classmethod
    def setUpClass(cls) -> None:
        config = {
            'hosts': [('0.0.0.0', 3000)]
        }
        cls._client = AioAerospikeClient(config)
        cls._query = cls._client.query("test", "test")
        # for i in range(100, 200):
        #     key = ("test", "test", i)
        #     asyncio.create_task(cls._client.put(
        #         key=key, bins={"a": i, "b": "test_query"}))


    async def test_execute_background(self):
        ops = [
            operations.increment("a", 100)
        ]
        self._query.add_ops(ops)
        await self._query.execute_background()

    # async def test_get_parts(self):
    #     return await self._query.get_parts()

    def test_max_records(self):
        return self._query.max_records

    def test_paginate(self):
        return self._query.paginate()

    def test_records_per_second(self):
        return self._query.records_per_second

    # async def test_results(self):
    #     return await self._query.results()

    def test_select(self):
        return self._query.select("a")

    def test_ttl(self):
        return self._query.ttl

    def test_where(self):
        return self._query.where(predicates.equals('a', 100))
