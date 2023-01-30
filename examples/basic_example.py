from aio_aerospike_python import AioAerospikeClient
from aio_aerospike_python import exception
from aerospike_helpers import expressions as exp
import aerospike


import asyncio
config = {
    'hosts': [('0.0.0.0', 3000)]
}

client = AioAerospikeClient(config)
print(client.is_connected())


async def put_some_data(limit: int):
    for i in range(limit):
        key = ("test", "test", i)
        data = {"a": i}
        await client.put(key, data)


async def read_data(limit: int):
    for i in range(limit):
        key = ("test", "test", i)
        r = await client.get(key)
        print(r)


async def use_query(mina: int, maxa: int):
    query = client.query("test", "test")
    expr = expr = exp.And(
        exp.LT(exp.IntBin("a"), maxa),
        exp.GT(exp.IntBin("a"), mina)
    ).compile()
    scan_policy = {"expressions": expr}
    results = await query.results(scan_policy)
    print("query results ===")
    for r in results:
        print(r)


async def use_scan(mina: int, maxa: int):
    scan = client.query("test", "test")
    expr = exp.And(
        exp.LT(exp.IntBin("a"), maxa),
        exp.GT(exp.IntBin("a"), mina)
    ).compile()
    scan_policy = {"expressions": expr}
    results = await scan.results(scan_policy)
    print("scan results ===")
    for r in results:
        print(r)


async def test_get_many():
    keys=[("test", "test", 2200001), ("test", "test", 2200002), ("test", "test", 2200003)]
    for k in keys:
        await client.put(key=k, bins={"a": "test"})
    results = await client.get_many(keys=keys)
    print(results)
    found = []
    for r in results:
        if r[2]:
            found.append(r)
    print(found)

loop = asyncio.get_event_loop()
loop.run_until_complete(put_some_data(5000))
loop.run_until_complete(read_data(50))
loop.run_until_complete(use_query(10,20))
loop.run_until_complete(use_scan(40, 45))
loop.run_until_complete(test_get_many())

# client.close()
