
import unittest
import aerospike
import asyncio
from aio_aerospike_python import AioAerospikeClient
from aerospike_helpers.batch.records import BatchRecords
from aio_aerospike_python import(
    INDEX_STRING,
    INDEX_NUMERIC,
    INDEX_TYPE_LIST)


class Connect(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        config = {
            'hosts': [('0.0.0.0', 3000)]
        }
        cls._client = AioAerospikeClient(config)

        # cls.assertTrue(cls.client.is_connected())
        cls.set = "test"
        cls.ns = "test"
        cls.key = ("test","test",1)
        cls.module = "mod1"
        cls.keys = [("test", "test", i) for i in range(1000) ]

    
    async def test_append(self, key=("test","test",3599587), bin="a", val="test", meta=None, policy=None):
        await self.test_put(key=key, bins={"vv":"test_"})
        await self._client.append(key=key, bin="vv", val="append", meta=meta, policy=policy)
        r = await self.test_get(key=key)
        key, _, bin = r
        self.assertEqual(bin.get("vv"),"test_append", "Append text works")
        # print (bin.get("vv"))
        
    # async def test_apply(self, key, module, function, args,policy=None):
    #     return await self._client.apply(key, module, function, args, policy)
        
    # async def test_batch_apply(self, keys, module, function, args, policy_batch, policy_batch_apply) :
    #     return await self._client.batch_apply(keys, module, function, args, policy_batch, policy_batch_apply) 
        
    # async def test_batch_get_ops(self, keys, ops, meta, policy) :
    #     return await self._client.batch_get_ops(keys, ops, meta, policy) 
        
    # async def test_batch_operate(self, keys, ops, policy_batch, policy_batch_write) :
    #     return await self._client.batch_operate(keys, ops, policy_batch, policy_batch_write) 
        
    # async def test_batch_remove(self, keys, policy_batch, policy_batch_remove) :
    #     return await self._client.batch_remove(keys, policy_batch, policy_batch_remove) 
        
    # async def test_batch_write(self, batch_records, policy) :
    #     return await self._client.batch_write(batch_records, policy) 
        
    # async def test_close(self):
    #     return await self._client.close()
        
    # async def test_connect(self, username, password):
    #     return await self._client.connect(username, password)
        
    async def test_exists(self, key=("test","test",1), policy=None) :
        await self.test_put(key=key, bins={"vv":"test_"})
        val =  await self._client.exists(key, policy) 
        
    async def test_exists_many(self, keys=[("test","test",1001),("test","test",1002)], policy=None) :
        for k in keys:
            await self._client.put(key=k, bins={"a":"test"})
        await self._client.exists_many(keys, policy) 
        for k in keys:
            await self._client.remove(key=k)

    async def test_get(self, key=("test","test",1002), policy=None) :
        await self.test_put(key=key, bins={"a":"test"})
        return await self._client.get(key, policy) 
        
    # async def test_get_async(self, get_callback, key, policy) :
    #     return await self._client.get_async(get_callback, key, policy) 
        
    # async def test_get_cdtctx_base64(self, compiled_cdtctx:list):
    #     return await self._client.get_cdtctx_base64(compiled_cdtctx) 
        
    # async def test_get_expression_base64(self, compiled_expression: list) :
    #     return await self._client.get_expression_base64(compiled_expression) 
        
    def test_get_key_digest(self, ns="test", set="test", key=1) :
        r = self._client.get_key_digest(ns, set, key) 
        self.assertEqual(r, b'\xf5\xf8o=HU\xad\xff\xe8\xde\xf9\xa0\xd9\x02\xfa\xc7\x1fW\x8b\x8f')
        # print(r)
    async def test_get_key_partition_id(self, ns="test", set="test", key=1) :
        r =  self._client.get_key_partition_id(ns, set, key) 
        
    async def test_get_many(self, keys=[("test","test",1005),("test","test",1006)], policy=None) :
        for k in keys:
            await self._client.put(key=k, bins={"a":"test"})
        results =  await self._client.get_many(keys=keys)
        # print(results) 
        # found = []
        # for r in results:
        #     if r[2]:
        #         found.append(r)
        # print(len(found))
        # self.assertEqual(len(results),2, "we got 2 results from get_many")
        # self.assertEqual(len(found),0, "we found 0  records")

    async def test_get_node_names(self) :
        r =  await self._client.get_node_names() 
        print(r)
    async def test_get_nodes(self) :
        return await self._client.get_nodes() 
        
    async def test_increment(self, key=("test","test",300001), bin="a", offset=2, meta=None, policy=None):
        await self.test_put(key=key, bins={"bfloat":3.6, "bint":5})
        await self._client.increment(key=key, bin="bfloat", offset=0.4)
        await self._client.increment(key=key, bin="bint", offset=5)
        r = await self.test_get(key=key)
        _,_,b = r
        self.assertEqual(b.get("bfloat"), 4.0, "increment float")
        self.assertEqual(b.get("bint"),10 , "increment int")
        
    # async def test_index_cdt_create(self, ns="test", set="test", bin="a",  index_type=aerospike.INDEX_STRING, index_datatype, index_name, ctx,policy=None):
    #     return await self._client.index_cdt_create(ns, set, bin,  index_type, index_datatype, index_name, ctx, policy)
        
    # async def test_index_geo2dsphere_create(self, ns="test", set="test", bin="a", index_name,policy=None):
    #     return await self._client.index_geo2dsphere_create(ns, set, bin, index_name, policy)
        
    async def test_index_integer_create(self, ns="test", set="test", bin="a", index_name="aidx", policy=None):
        return await self._client.index_integer_create(ns, set, bin, index_name, policy)
        
    async def test_index_list_create(self, ns="test", set="test", bin="b", index_datatype=INDEX_STRING, index_name="bidx", policy=None):
        return await self._client.index_list_create(ns, set, bin, index_datatype, index_name, policy)
        
    async def test_index_map_keys_create(self, ns="test", set="test", bin="a", index_datatype=INDEX_STRING, index_name="mapkeytest", policy=None):
        return await self._client.index_map_keys_create(ns, set, bin, index_datatype, index_name, policy)
        
    async def test_index_map_values_create(self, ns="test", set="test", bin="a", index_datatype=INDEX_STRING, index_name="mapvaluetest", policy=None):
        return await self._client.index_map_values_create(ns, set, bin, index_datatype, index_name, policy)
        
    async def test_index_remove(self, ns="test", index_name="bidx", policy=None):
        return await self._client.index_remove(ns, index_name, policy)
        
    async def test_index_string_create(self, ns="test", set="test", bin="c", index_name="stridx", policy=None):
        return await self._client.index_string_create(ns, set, bin, index_name, policy)
        
    async def test_info(self, command="alumni-clear-std", hosts=["localhost"], policy=None) :
        return await self._client.info(command, hosts, policy) 
        
    async def test_info_all(self, command="alumni-clear-std", policy=None) :
        return await self._client.info_all(command, policy) 
        
    # async def test_info_node(self, command, host, policy) :
    #     return await self._client.info_node(command, host, policy) 
        
    # async def test_info_random_node(self, command, policy) :
    #     return await self._client.info_random_node(command, policy) 
        
    # async def test_info_single_node(self, command, host, policy) :
    #     return await self._client.info_single_node(command, host, policy) 
        
    # async def test_is_connected(self):
    #     return await self._client.is_connected()
        
    # async def test_job_info(self, job_id, module, policy) :
    #     return await self._client.job_info(job_id, module, policy) 
        
    # async def test_list_append(self, key=("test","test",3), bin="b", val=2, meta=None, policy=None):
    #     return await self._client.list_append(key, bin, val, meta, policy)
        
    # async def test_list_clear(self, key, bin, meta=None, policy=None):
    #     return await self._client.list_clear(key, bin, meta, policy)
        
    # async def test_list_extend(self, key, bin, items, meta=None, policy=None):
    #     return await self._client.list_extend(key, bin, items, meta, policy)
        
    # async def test_list_get(self, key, bin, index, meta, policy) :
    #     return await self._client.list_get(key, bin, index, meta, policy) 
        
    # async def test_list_get_range(self, key, bin, index, count, meta, policy) :
    #     return await self._client.list_get_range(key, bin, index, count, meta, policy) 
        
    # async def test_list_insert(self, key, bin, index, val, meta=None, policy=None):
    #     return await self._client.list_insert(key, bin, index, val, meta, policy)
        
    # async def test_list_insert_items(self, key, bin, index, items, meta=None, policy=None):
    #     return await self._client.list_insert_items(key, bin, index, items, meta, policy)
        
    # async def test_list_pop(self, key, bin, index, meta, policy) :
    #     return await self._client.list_pop(key, bin, index, meta, policy) 
        
    # async def test_list_pop_range(self, key, bin, index, count, meta, policy) :
    #     return await self._client.list_pop_range(key, bin, index, count, meta, policy) 
        
    # async def test_list_remove(self, key, bin, index, meta=None, policy=None):
    #     return await self._client.list_remove(key, bin, index, meta, policy)
        
    # async def test_list_remove_range(self, key, bin, index, count, meta=None, policy=None):
    #     return await self._client.list_remove_range(key, bin, index, count, meta, policy)
        
    # async def test_list_set(self, key, bin, index, val, meta=None, policy=None):
    #     return await self._client.list_set(key, bin, index, val, meta, policy)
        
    # async def test_list_size(self, key, bin, meta, policy) :
    #     return await self._client.list_size(key, bin, meta, policy) 
        
    # async def test_list_trim(self, key, bin, index, count, meta, policy) :
    #     return await self._client.list_trim(key, bin, index, count, meta, policy) 
        
    # async def test_map_clear(self, key, bin, meta=None, policy=None):
    #     return await self._client.map_clear(key, bin, meta, policy)
        
    # async def test_map_decrement(self, key, bin, map_key, decr, map_policy=None, meta=None, policy=None):
    #     return await self._client.map_decrement(key, bin, map_key, decr, map_policy, meta, policy)
        
    # async def test_map_get_by_index(self, key, bin, index, return_type, meta=None, policy=None):
    #     return await self._client.map_get_by_index(key, bin, index, return_type, meta, policy)
        
    # async def test_map_get_by_index_range(self, key, bin, index, range, return_type, meta=None, policy=None):
    #     return await self._client.map_get_by_index_range(key, bin, index, range, return_type, meta, policy)
        
    # async def test_map_get_by_key(self, key, bin, map_key, return_type, meta=None, policy=None):
    #     return await self._client.map_get_by_key(key, bin, map_key, return_type, meta, policy)
        
    # async def test_map_get_by_value_range(self, key, bin, key_list, return_type, meta=None, policy=None):
    #     return await self._client.map_get_by_value_range(key, bin, key_list, return_type, meta, policy)
        
    # async def test_map_get_by_key_range(self, key, bin, map_key, range, return_type, meta=None, policy=None):
    #     return await self._client.map_get_by_key_range(key, bin, map_key, range, return_type, meta, policy)
        
    # async def test_map_get_by_rank(self, key, bin, rank, return_type, meta=None, policy=None):
    #     return await self._client.map_get_by_rank(key, bin, rank, return_type, meta, policy)
        
    # async def test_map_get_by_rank_range(self, key, bin, rank, range, return_type, meta=None, policy=None):
    #     return await self._client.map_get_by_rank_range(key, bin, rank, range, return_type, meta, policy)
        
    # async def test_map_get_by_value(self, key, bin, val, return_type, meta=None, policy=None):
    #     return await self._client.map_get_by_value(key, bin, val, return_type, meta, policy)
        
    # async def test_map_get_by_value_range(self, key, bin, value_list, return_type, meta=None, policy=None):
    #     return await self._client.map_get_by_value_range(key, bin, value_list, return_type, meta, policy)
        
    # async def test_map_get_by_value_range(self, key, bin, val, range, return_type, meta=None, policy=None):
    #     return await self._client.map_get_by_value_range(key, bin, val, range, return_type, meta, policy)
        
    # async def test_map_increment(self, key, bin, map_key, incr, map_policy=None, meta=None, policy=None):
    #     return await self._client.map_increment(key, bin, map_key, incr, map_policy, meta, policy)
        
    # async def test_map_put(self, key, bin, map_key, val, map_policy=None, meta=None, policy=None):
    #     return await self._client.map_put(key, bin, map_key, val, map_policy, meta, policy)
        
    # async def test_map_put_items(self, key, bin, items, map_policy=None, meta=None, policy=None):
    #     return await self._client.map_put_items(key, bin, items, map_policy, meta, policy)
        
    # async def test_map_remove_by_index(self, key, bin, index, return_type, meta=None, policy=None):
    #     return await self._client.map_remove_by_index(key, bin, index, return_type, meta, policy)
        
    # async def test_map_remove_by_index_range(self, key, bin, index, range, return_type, meta=None, policy=None):
    #     return await self._client.map_remove_by_index_range(key, bin, index, range, return_type, meta, policy)
        
    # async def test_map_remove_by_key(self, key, bin, map_key, return_type, meta=None, policy=None):
    #     return await self._client.map_remove_by_key(key, bin, map_key, return_type, meta, policy)
        
    # async def test_map_remove_by_key_list(self, key, bin, list, return_type, meta=None, policy=None):
    #     return await self._client.map_remove_by_key_list(key, bin, list, return_type, meta, policy, meta, policy)
        
    # async def test_map_remove_by_key_range(self, key, bin, map_key, range, return_type, meta=None, policy=None):
    #     return await self._client.map_remove_by_key_range(key, bin, map_key, range, return_type, meta, policy)
        
    # async def test_map_remove_by_rank(self, key, bin, rank, return_type, meta=None, policy=None):
    #     return await self._client.map_remove_by_rank(key, bin, rank, return_type, meta, policy)
        
    # async def test_map_remove_by_rank_range(self, key, bin, rank, range, return_type, meta=None, policy=None):
    #     return await self._client.map_remove_by_rank_range(key, bin, rank, range, return_type, meta, policy)
        
    # async def test_map_remove_by_value(self, key, bin, val, return_type, meta=None, policy=None):
    #     return await self._client.map_remove_by_value(key, bin, val, return_type, meta, policy)
        
    # async def test_map_remove_by_value_list(self, key, bin, list, return_type, meta=None, policy=None):
    #     return await self._client.map_remove_by_value_list(key, bin, list, return_type, meta, policy)
        
    # async def test_map_remove_by_value_range(self, key, bin, val, range, return_type, meta=None, policy=None):
    #     return await self._client.map_remove_by_value_range(key, bin, val, range, return_type, meta, policy)
        
    # async def test_map_set_policy(self, key, bin, map_policy):
    #     return await self._client.map_set_policy(key, bin, map_policy)
        
    # async def test_map_size(self, key, bin, meta, policy) :
    #     return await self._client.map_size(key, bin, meta, policy) 
        
    # async def test_operate(self, key, list, meta, policy) :
    #     return await self._client.operate(key, list, meta, policy) 
        
    # async def test_operate_ordered(self, key, list, meta, policy) :
    #     return await self._client.operate_ordered(key, list, meta, policy) 
        
    async def test_prepend(self, key=("test","test",100000000), bin="c", val="test", meta=None, policy=None):
        return await self._client.prepend(key, bin, val, meta, policy)
        
    async def test_put(self, key=("test","test",100000000), bins={"a":1,"c":"aaaa"}, meta=None, policy=None, serializer=None):
        return await self._client.put(key, bins, meta, policy, serializer)
               
    # def test_query(self, namespace, set) :
    #     return self._client.query(namespace, set) 
        
    # async def test_query_apply(self, ns="test", set="test", predicate=None, module=None, function=None, args=None, policy=None) :
    #     return await self._client.query_apply(ns, set, predicate, module, function, args, policy) 
        
    # async def test_remove(self, key,policy=None):
    #     return await self._client.remove(key, policy)
        
    # async def test_remove_bin(self, key, list, meta=None, policy=None):
    #     return await self._client.remove_bin(key, list, meta, policy)
        
    # async def test_scan(self, namespace, set) :
    #     return await self._client.scan(namespace, set) 
        
    # async def test_scan_apply(self, ns="test", set="test", module=None, function=None, args=None, policy=None, options=None, block=None) :
    #     return await self._client.scan_apply(ns, set, module, function, args, policy, options, block) 
        
    # async def test_scan_info(self, scan_id) :
    #     return await self._client.scan_info(scan_id) 
        
    # async def test_select(self, key=("test","test",1), bins=["a"], policy=None) :
    #     return await self._client.select(key, bins, policy) 
        
    # async def test_select_many(self, keys, bins, policy) :
    #     return await self._client.select_many(keys, bins, policy) 
        
    # async def test_set_xdr_filter(self, data_center, namespace, expression_filter, policy) :
    #     return await self._client.set_xdr_filter(data_center, namespace, expression_filter, policy) 
        
    # async def test_shm_key(self):
    #     return await self._client.shm_key()
        
    async def test_touch(self, key=("test","test",1008), val=0, meta=None, policy=None):
        await self.test_put(key=key)
        return await self._client.touch(key, val, meta, policy)
        
    # async def test_truncate(self, namespace, set, nanos,policy=None):
    #     return await self._client.truncate(namespace, set, nanos, policy)
        
    # async def test_udf_get(self, module, language, policy) :
    #     return await self._client.udf_get(module, language, policy) 
        
    # async def test_udf_list(self, policy) :
    #     return await self._client.udf_list(policy) 
        
    # async def test_udf_put(self, filename, udf_type,policy=None):
    #     return await self._client.udf_put(filename, udf_type, policy)
        
    # async def test_udf_remove(self, module,policy=None):
    #     return await self._client.udf_remove(module, policy)


        