
import aerospike
import asyncio
from functools import partial
from typing import Union
from aio_aerospike_python.aio_aerospike_query import AioAerospikeQuery
from aio_aerospike_python.aio_aerospike_scan import AioAerospikeScan
from aerospike_helpers.batch.records import BatchRecords


class AioAerospikeClient():
    def __init__(self, config: dict, username: str = None, password: str = None) -> None:
        if username and password:
            self._client = aerospike.Client(config).connect(username, password)
        else:
            self._client = aerospike.Client(config).connect()

    async def append(self, key: tuple, bin: str, val: str, meta: dict, policy: dict):
        '''Append the string val to the string value in bin.
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.append, key, bin, val, meta, policy))

    async def apply(self, key, module, function, args=None, policy=None):
        '''Apply a registered (see udf_put()) record UDF to a particular record.
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.apply, key, module, function, args, policy))

    async def batch_apply(self, keys: list, module: str, function: str, args: list, policy_batch: dict = None, policy_batch_apply: dict = None) -> BatchRecords:
        '''Apply a user defined function (UDF) to multiple keys. Requires server version 6.0+
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.batch_apply, keys, module, function, args, policy_batch, policy_batch_apply))

    async def batch_get_ops(self, keys, ops, meta, policy) -> list:
        '''Batch-read multiple records, and return them as a list. Any record that does not exist will have a exception type value as metadata and None value as bin in the record tuple.
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.batch_get_ops, keys, ops, meta, policy))

    async def batch_operate(self, keys: list, ops: list, policy_batch: dict = None, policy_batch_write: dict = None) -> BatchRecords:
        '''Perform read/write operations on multiple keys. Requires server version 6.0+
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.batch_operate, keys, ops, policy_batch, policy_batch_write))

    async def batch_remove(self, keys: list, policy_batch: dict = None, policy_batch_remove: dict = None) -> BatchRecords:
        '''Remove multiple records by key. Requires server version 6.0+
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.batch_remove, keys, policy_batch, policy_batch_remove))

    async def batch_write(self, batch_records: BatchRecords, policy: dict = None) -> BatchRecords:
        '''Read/Write multiple records for specified batch keys in one batch call. This method allows different sub-commands for each key in the batch. The returned records are located in the same list. Requires server version 6.0+
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.batch_write, batch_records, policy))

    def close(self):
        '''
        '''
        return self._client.close()

    async def exists(self, key: tuple, policy: dict = None) -> tuple:
        '''Check if a record with a given key exists in the cluster and return the record as a tuple() consisting of key and meta. If the record does not exist the meta data will be None.
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.exists, key, policy))

    async def exists_many(self, keys: list[tuple], policy: dict = None) -> list:
        '''Batch-read metadata for multiple keys, and return it as a list. Any record that does not exist will have a None value for metadata in the result tuple.
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.exists_many, keys, policy))

    async def get(self, key: tuple, policy: dict = None) -> tuple:
        '''Read a record with a given key, and return the record as a tuple() consisting of key, meta and bins.
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.get, key, policy))

    def get_cdtctx_base64(self, compiled_cdtctx: list) -> str:
        '''Get the base64 representation of a compiled aerospike CDT ctx.
        '''
        return self._client.get_cdtctx_base64(compiled_cdtctx)

    def get_expression_base64(self, compiled_expression: list) -> str:
        '''Get the base64 representation of a compiled aerospike expression.
        '''
        return self._client.get_expression_base64(compiled_expression)

    def get_key_digest(self, ns: str, set: str, key: Union[str, int]) -> bytearray:
        '''Calculate the digest of a particular key. See: Key Tuple.
        '''
        return self._client.get_key_digest(ns, set, key)

    def get_key_partition_id(self, ns: str, set: str, key: Union[str, int]) -> int:
        '''Gets the partition ID of given key. See: Key Tuple.
        '''
        return self._client.get_key_partition_id(ns, set, key)

    async def get_many(self, keys: list, policy: dict = None) -> list:
        '''Batch-read multiple records with applying list of operations and returns them as a list. Any record that does not exist will have a None value for metadata and status in the record tuple.
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.get_many, keys, policy))

    async def get_node_names(self) -> list:
        '''Return the list of hosts, including node names, present in a connected cluster.
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.get_node_names))

    async def get_nodes(self) -> list:
        '''Return the list of hosts present in a connected cluster.
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.get_nodes))

    async def increment(self, key: tuple, bin: str, offset: Union[int, float], meta: dict = None, policy: dict = None):
        '''Increment the integer value in bin by the integer val.
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.increment, key, bin, offset, meta, policy))

    async def index_cdt_create(self, ns: str, set: str, bin: str, index_type, index_datatype, index_name, ctx, policy):
        '''Create an cdt index named index_name for list, map keys or map values (as defined by index_type) and for numeric, string or GeoJSON values (as defined by index_datatype) on records of the specified ns, set whose bin is a list or map.
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.index_cdt_create, ns, set, bin, index_type, index_datatype, index_name, ctx, policy))

    async def index_geo2dsphere_create(self, ns, set, bin, index_name: str, policy: dict = None):
        '''Create a geospatial 2D spherical index with index_name on the bin in the specified ns, set.
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.index_geo2dsphere_create, ns, set, bin, index_name, policy))

    async def index_integer_create(self, ns: str, set: str, bin: str, index_name: str, policy: dict = None):
        '''Create an integer index with index_name on the bin in the specified ns, set.
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.index_integer_create, ns, set, bin, index_name, policy))

    async def index_list_create(self, ns, set, bin, index_datatype, index_name, policy):
        '''Create an index named index_name for numeric, string or GeoJSON values (as defined by index_datatype) on records of the specified ns, set whose bin is a list.
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.index_list_create, ns, set, bin, index_datatype, index_name, policy))

    async def index_map_keys_create(self, ns, set, bin, index_datatype, index_name, policy=None):
        '''Create an index named index_name for numeric, string or GeoJSON values (as defined by index_datatype) on records of the specified ns, set whose bin is a map. The index will include the keys of the map.
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.index_map_keys_create, ns, set, bin, index_datatype, index_name, policy))

    async def index_map_values_create(self, ns, set, bin, index_datatype, index_name, policy=None):
        '''Create an index named index_name for numeric, string or GeoJSON values (as defined by index_datatype) on records of the specified ns, set whose bin is a map. The index will include the values of the map.
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.index_map_values_create, ns, set, bin, index_datatype, index_name, policy))

    async def index_remove(self, ns, index_name, policy=None):
        '''Remove the index with index_name from the namespace.
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.index_remove,
                                                  ns, index_name, policy))

    async def index_string_create(self, ns, set, bin, index_name, policy=None):
        '''Create a string index with index_name on the bin in the specified ns, set.
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.index_string_create, ns, set, bin, index_name, policy))

    async def info(self, command: str, hosts: list = None, policy: dict = None) -> dict:
        '''Send an info command to multiple nodes specified in a hosts list.
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.info, command, hosts, policy))

    async def info_all(self, command: str, policy=None) -> dict:
        '''Send an info *command* to all nodes in the cluster to which the client is connected.
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.info_all, command, policy))

    async def info_node(self, command, host: str, policy=None) -> str:
        '''DEPRECATED: Please user info_single_node() instead.
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.info_node, command, host, policy))

    async def info_random_node(self, command, policy=None) -> str:
        '''Send an info command to a single random node.
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.info_random_node, command, policy))

    async def info_single_node(self, command, host: str, policy=None) -> str:
        '''Send an info command to a single node specified by host.
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.info_single_node, command, host, policy))

    def is_connected(self):
        '''
        '''
        return self._client.is_connected()

    async def job_info(self, job_id, module, policy=None) -> dict:
        '''Return the status of a job running in the background.
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.job_info, job_id, module, policy))

    async def list_append(self, key, bin, val, meta=None, policy=None):
        '''Append a single element to a list value in bin.
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.list_append, key, bin, val, meta, policy))

    async def list_clear(self, key: tuple, bin: str, meta=None, policy=None):
        '''Remove all the elements from a list value in bin.
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.list_clear, key, bin, meta, policy))

    async def list_extend(self, key, bin, items, meta=None, policy=None):
        '''Extend the list value in bin with the given items.
            '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.list_extend, key, bin, items, meta, policy))

    async def list_get(self, key, bin, index, meta=None, policy=None):
        '''Get the list element at the specified index of a list value in bin.
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.list_get, key, bin, index, meta, policy))

    async def list_get_range(self, key, bin, index, count, meta=None, policy=None):
        '''Get the list of count elements starting at a specified index of a list value in bin.
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.list_get_range, key, bin, index, count, meta, policy))

    async def list_insert(self, key, bin, index, val, meta=None, policy=None):
        '''Insert an element at the specified index of a list value in bin.
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.list_insert, key, bin, index, val, meta, policy))

    async def list_insert_items(self, key, bin, index, items, meta=None, policy=None):
        '''Insert the items at the specified index of a list value in bin.
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.list_insert_items, key, bin, index, items, meta, policy))

    async def list_pop(self, key, bin, index, meta=None, policy=None):
        '''Remove and get back a list element at a given index of a list value in bin.
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.list_pop, key, bin, index, meta, policy))

    async def list_pop_range(self, key, bin, index, count, meta=None, policy=None):
        '''Remove and get back list elements at a given index of a list value in bin.
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.list_pop_range, key, bin, index, count, meta, policy))

    async def list_remove(self, key, bin, index, meta: dict = None, policy: dict = None):
        '''Remove a list element at a given index of a list value in bin.
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.list_remove, key, bin, index, meta, policy))

    async def list_remove_range(self, key, bin, index, count, meta: dict = None, policy: dict = None):
        '''Remove list elements at a given index of a list value in bin.
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.list_remove_range, key, bin, index, count, meta, policy))

    async def list_set(self, key, bin, index, val, meta: dict = None, policy: dict = None):
        '''Set list element val at the specified index of a list value in bin.
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.list_set, key, bin, index, val, meta, policy))

    async def list_size(self, key, bin, meta: dict = None, policy: dict = None):
        '''Count the number of elements in the list value in bin.
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.list_size, key, bin, meta, policy))

    async def list_trim(self, key, bin, index, count, meta: dict = None, policy: dict = None):
        '''Remove elements from the list which are not within the range starting at the given index plus count.
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.list_trim, key, bin, index, count, meta, policy))

    async def map_clear(self, key, bin, meta: dict = None, policy: dict = None):
        '''Remove all entries from the map specified by key and bin.
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.map_clear, key, bin, meta, policy))

    async def map_decrement(self, key, bin, map_key, decr, map_policy: dict = None, meta: dict = None, policy: dict = None):
        '''Decrement the value of the map entry by given decr. Map entry is specified by key, bin and map_key.
            '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.map_decrement, key, bin, map_key, decr, map_policy, meta, policy))

    async def map_get_by_index(self, key, bin, index, return_type, meta: dict = None, policy: dict = None):
        '''Return the map entry from the map specified by key and bin at the given index location.
            '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.map_get_by_index, key, bin, index, return_type, meta, policy))

    async def map_get_by_index_range(self, key, bin, index, range, return_type, meta: dict = None, policy: dict = None):
        '''Return the map entries from the map specified by key and bin starting at the given index location and removing range number of items.
            '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.map_get_by_index_range, key, bin, index, range, return_type, meta, policy))

    async def map_get_by_key(self, key, bin, map_key, return_type, meta: dict = None, policy: dict = None):
        '''Return map entry from the map specified by key and bin which has a key that matches the given map_key.
            '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.map_get_by_key, key, bin, map_key, return_type, meta, policy))

    async def map_get_by_value_range(self, key, bin, key_list, return_type, meta: dict = None, policy: dict = None):
        '''Return map entries from the map specified by key and bin for keys matching those in the provided key_list.
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.map_get_by_value_range, key, bin, key_list, return_type, meta, policy))

    async def map_get_by_key_range(self, key, bin, map_key, range, return_type, meta: dict = None, policy: dict = None):
        '''Return map entries from the map specified by key and bin identified by the key range (map_key inclusive, range exclusive).
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.map_get_by_key_range, key, bin, map_key, range, return_type, meta, policy))

    async def map_get_by_rank(self, key, bin, rank, return_type, meta: dict = None, policy: dict = None):
        '''Return the map entry from the map specified by key and bin with a value that has the given rank.
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.map_get_by_rank, key, bin, rank, return_type, meta, policy))

    async def map_get_by_rank_range(self, key, bin, rank, range, return_type, meta: dict = None, policy: dict = None):
        '''Return the map entries from the map specified by key and bin which have a value rank starting at rank and removing range number of items.
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.map_get_by_rank_range, key, bin, rank, range, return_type, meta, policy))

    async def map_get_by_value(self, key, bin, val, return_type, meta: dict = None, policy: dict = None):
        '''Return map entries from the map specified by key and bin which have a value matching val parameter.
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.map_get_by_value, key, bin, val, return_type, meta, policy))

    async def map_get_by_value_range(self, key, bin, value_list, return_type, meta: dict = None, policy: dict = None):
        '''Return map entries from the map specified by key and bin which contain a value matching one of the values in the provided value_list.
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.map_get_by_value_range, key, bin, value_list, return_type, meta, policy))

    async def map_get_by_value_range(self, key, bin, val, range, return_type, meta: dict = None, policy: dict = None):
        '''Return map entries from the map specified by key and bin identified by the value range (val inclusive, range exclusive).
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.map_get_by_value_range, key, bin, val, range, return_type, meta, policy))

    async def map_increment(self, key, bin, map_key, incr, map_policy: dict = None, meta: dict = None, policy: dict = None):
        '''Increment the value of the map entry by given incr. Map entry is specified by key, bin and map_key.
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.map_increment, key, bin, map_key, incr, map_policy, meta, policy))

    async def map_put(self, key, bin, map_key, val, map_policy: dict = None, meta: dict = None, policy: dict = None):
        '''Add the given map_key/value pair to the map record specified by key and bin.
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.map_put, key, bin, map_key, val, map_policy, meta, policy))

    async def map_put_items(self, key, bin, items, map_policy: dict = None, meta: dict = None, policy: dict = None):
        '''Add the given items dict of key/value pairs to the map record specified by key and bin.
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.map_put_items, key, bin, items, map_policy, meta, policy))

    async def map_remove_by_index(self, key, bin, index, return_type, meta: dict = None, policy: dict = None):
        '''Remove and optionally return the map entry from the map specified by key and bin at the given index location.
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.map_remove_by_index, key, bin, index, return_type, meta, policy))

    async def map_remove_by_index_range(self, key, bin, index, range, return_type, meta: dict = None, policy: dict = None):
        '''Remove and optionally return the map entries from the map specified by key and bin starting at the given index location and removing range number of items.
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.map_remove_by_index_range, key, bin, index, range, return_type, meta, policy))

    async def map_remove_by_key(self, key, bin, map_key, return_type, meta: dict = None, policy: dict = None):
        '''Remove and optionally return first map entry from the map specified by key and bin which matches given map_key.
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.map_remove_by_key, key, bin, map_key, return_type, meta, policy))

    async def map_remove_by_key_range(self, key, bin, map_key, range, return_type, meta: dict = None, policy: dict = None):
        '''Remove and optionally return map entries from the map specified by key and bin identified by the key range (map_key inclusive, range exclusive).
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.map_remove_by_key_range, key, bin, map_key, range, return_type, meta, policy))

    async def map_remove_by_rank(self, key, bin, rank, return_type, meta: dict = None, policy: dict = None):
        '''Remove and optionally return the map entry from the map specified by key and bin with a value that has the given rank.
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.map_remove_by_rank, key, bin, rank, return_type, meta, policy))

    async def map_remove_by_rank_range(self, key, bin, rank, range, return_type, meta: dict = None, policy: dict = None):
        '''Remove and optionally return the map entries from the map specified by key and bin which have a value rank starting at rank and removing range number of items.
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.map_remove_by_rank_range, key, bin, rank, range, return_type, meta, policy))

    async def map_remove_by_value(self, key, bin, val, return_type, meta: dict = None, policy: dict = None):
        '''Remove and optionally return map entries from the map specified by key and bin which have a value matching val parameter.
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.map_remove_by_value, key, bin, val, return_type, meta, policy))

    async def map_remove_by_value_list(self, key, bin, list, return_type, meta: dict = None, policy: dict = None):
        '''Remove and optionally return map entries from the map specified by key and bin which have a value matching the list of values.
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.map_remove_by_value_list, key, bin, list, return_type, meta, policy))

    async def map_remove_by_value_range(self, key, bin, val, range, return_type, meta: dict = None, policy: dict = None):
        '''Remove and optionally return map entries from the map specified by key and bin identified by the value range (val inclusive, range exclusive).
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.map_remove_by_value_range, key, bin, val, range, return_type, meta, policy))

    async def map_set_policy(self, key, bin, map_policy):
        '''Set the map policy for the given bin.
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.map_set_policy, key, bin, map_policy))

    async def map_size(self, key, bin, meta: dict = None, policy: dict = None):
        '''Return the size of the map specified by key and bin.
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.map_size, key, bin, meta, policy))

    async def operate(self, key, list, meta: dict = None, policy: dict = None) -> tuple:
        '''Perform multiple bin operations on a record with a given key, In Aerospike server versions prior to 3.6.0, non-existent bins being read will have a None value. Starting with 3.6.0 non-existent bins will not be present in the returned Record Tuple. The returned record tuple will only contain one entry per bin, even if multiple operations were performed on the bin.
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.operate, key, list, meta, policy))

    async def operate_ordered(self, key, list, meta: dict = None, policy: dict = None) -> tuple:
        '''Perform multiple bin operations on a record with the results being returned as a list of (bin-name, result) tuples. The order of the elements in the list will correspond to the order of the operations from the input parameters.
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.operate_ordered, key, list, meta, policy))

    async def prepend(self, key, bin, val, meta: dict = None, policy: dict = None):
        '''Prepend the string value in bin with the string val.
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.prepend, key, bin, val, meta, policy))

    async def put(self, key: tuple, bins: dict, meta: dict = None, policy: dict = None, serializer=aerospike.SERIALIZER_PYTHON):
        '''Write a record asynchronously with a given key to the cluster.
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.put, key, bins, meta, policy, serializer))

    def query(self, namespace: str, set: str = None) -> AioAerospikeQuery:
        '''Return a `aerospike.Query` object to be used for executing queries over a specified set (which can be omitted or None) in a namespace. A query with a None set returns records which are not in any named set. This is different than the meaning of a None set in a scan.
        '''
        q = self._client.query(namespace=namespace, set=set)
        return AioAerospikeQuery(q)

    async def query_apply(self, ns, set, predicate, module, function, args: list = None, policy: dict = None) -> int:
        '''Initiate a background query and apply a record UDF to each record matched by the query.
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.query_apply, ns, set, predicate, module, function, args, policy))

    async def remove(self, key, policy: dict = None):
        '''Remove a record matching the key from the cluster.
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.remove, key, policy))

    async def remove_bin(self, key, list, meta: dict = None, policy: dict = None):
        '''Remove a list of bins from a record with a given key. Equivalent to setting those bins to aerospike.null() with a put().
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.remove_bin, key, list, meta, policy))

    def scan(self, namespace: str, set: str = None) -> AioAerospikeScan:
        '''Return a `aerospike.Scan` object to be used for executing scans over a specified set (which can be omitted or None) in a namespace. A scan with a None set returns all the records in the namespace.
        '''
        scan = self._client.scan(namespace, set)
        return AioAerospikeScan(scan)

    async def scan_apply(self, ns: str, set: str, module: str, function: str, args: list = None, policy=None, options=None, block=None) -> int:
        '''Initiate a background scan and apply a record UDF to each record matched by the scan.
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.scan_apply, ns, set, module, function, args, policy, options, block))

    async def scan_info(self, scan_id) -> dict:
        '''Return the status of a scan running in the background.
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.scan_info, scan_id))

    async def select(self, key: tuple, bins: list, policy: dict = None) -> tuple:
        '''Read a record with a given key, and return the record as a tuple() consisting of key, meta and bins, with the specified bins projected. Prior to Aerospike server 3.6.0, if a selected bin does not exist its value will be None. Starting with 3.6.0, if a bin does not exist it will not be present in the returned Record Tuple.

        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.select, key, bins, policy))

    async def select_many(self, keys: list, bins: list, policy: dict = None) -> list:
        '''Batch-read multiple records, and return them as a list. Any record that does not exist will have a None value for metadata and bins in the record tuple. The bins will be filtered as specified.
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.select_many, keys, bins, policy))

    async def set_xdr_filter(self, data_center, namespace, expression_filter, policy: dict = None) -> dict:
        '''Set cluster xdr filter.
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.set_xdr_filter, data_center, namespace, expression_filter, policy))

    def shm_key(self):
        return self._client.shm_key()

    async def touch(self, key=None, val=0, meta: dict = None, policy: dict = None):
        '''Touch the given record, resetting its time-to-live and incrementing its generation.
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.touch, key, val, meta, policy))

    async def truncate(self, namespace, set, nanos, policy: dict = None):
        '''Remove records in specified namespace/set efficiently. This method is many orders of magnitude faster than deleting records one at a time. Works with Aerospike Server versions >= 3.12.
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.truncate, namespace, set, nanos, policy))

    async def udf_get(self, module, language=None, policy=None) -> str:
        '''Return the content of a UDF module which is registered with the cluster.
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.udf_get, module, language, policy))

    async def udf_list(self, policy=None) -> list:
        '''Return the list of UDF modules registered with the cluster.
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.udf_list, policy))

    async def udf_put(self, filename, udf_type=None, policy=None):
        '''Register a UDF module with the cluster.
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.udf_put, filename, udf_type, policy))

    async def udf_remove(self, module, policy: dict = None):
        '''Remove a  previously registered UDF module from the cluster.
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._client.udf_remove, module, policy))
