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




loop = asyncio.get_event_loop()
loop.run_until_complete(put_some_data(5000))
loop.run_until_complete(read_data(50))
loop.run_until_complete(use_query(10,20))
loop.run_until_complete(use_scan(40, 45))

# client.close()
