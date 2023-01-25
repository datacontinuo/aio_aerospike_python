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

# client.close()



