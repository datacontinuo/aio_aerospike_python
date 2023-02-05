Quickstart
==========

The aio-aerospike-python is an aio wrapper over the Aerospike python client.
It is like what motor package is for pymongo.


install
-------

pip install aio-aerospike-python

first code snippet 
------------------
Lets try and add a few keys to aerospike database 


.. code::

    from aio_aerospike_python import AioAerospikeClient
    import asyncio
    config = {
        'hosts': [('0.0.0.0', 3000)]
    }

    client = AioAerospikeClient(config)

    async def put_some_data(limit: int):
        for i in range(limit):
            key = ("test", "test", i)
            data = {"a": i}
            await client.put(key, data)


    loop = asyncio.get_event_loop()
    loop.run_until_complete(put_some_data(100))



