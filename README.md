# asyncio wrapper of aerospike python client library #



<a href="https://github.com/datacontinuo-com/aio_aerospike_python" target="_blank">
    <img src="https://img.shields.io/github/last-commit/datacontinuo-com/aio_aerospike_python" alt="Latest Commit">
</a>
<img src="https://img.shields.io/badge/License-Apache_2.0-blue.svg">
<a href="https://github.com/datacontinuo-com/aio_aerospike_python/issues"><img src="https://img.shields.io/github/issues/datacontinuo-com/aio_aerospike_python" /></a>
<img alt="Contributors" src="https://img.shields.io/github/contributors/datacontinuo-com/aio_aerospike_python">
<a href="https://pypi.org/project/aio-aerospike-python/" target="_blank">
    <img src="https://img.shields.io/pypi/v/aio-aerospike-python" alt="Package version">
</a>
<img src="https://img.shields.io/pypi/pyversions/aio-aerospike-python">


This project is work in progress. please do not use it in production yet. 




This project provides a simple way to use aerospike with asyncio. 

This project is based on  [Aerospike python client library docs](https://aerospike-python-client.readthedocs.io/en/latest/index.html) 
[Docs](https://github.com/aerospike/aerospike-client-python)

## installation ##

```bash
pip install aio-aerospike-python
```


## Quick start  ##
start docker compose 

```bash
docker compose up -d 
```

```python
from aio_aerospike_python import AioAerospikeClient
import asyncio
config = {
    'hosts': [('0.0.0.0', 3000)]
}
client = AioAerospikeClient(config)
print(client.is_connected())
async def put_some_data(limit:int):
    for i in range(limit):
        key = ("test","test",i)
        data = {"a":i}
        await client.put(key,data)


async def read_data(limit:int):
    for i in range(limit):
        key = ("test","test",i)
        r = await client.get(key)
        print(r)


loop = asyncio.get_event_loop()
loop.run_until_complete(put_some_data(33))
loop.run_until_complete(read_data(33))

client.close()
```

Now lets test it with concurrency
```python
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
    keys = [("test", "test", i) for i in range(limit) ]
    # print(keys)
    r = await client.get_many(keys)
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



async def test_append(key=("test","test",3), bin="a", val="test", meta=None, policy=None):
    await client.put(key=key, bins={"vv":"test_"})
    await client.append(key=key, bin="vv", val="append", meta=meta, policy=policy)
    r = await client.get(key=key)
    key, _, bin = r
    print("append")
    print(r)

async def main():
    L = await asyncio.gather(
        put_some_data(700),
        read_data(50),
        use_query(10, 20),
        use_scan(40, 45),
        test_append()
    )

asyncio.run(main())
```


## License ##

The AIO Aerospike Python Client is made available under the terms of the Apache License, Version 2, as stated in the file LICENSE.
