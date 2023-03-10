import aerospike
import asyncio
from functools import partial
from typing import List, Dict


class AioAerospikeQuery():
    def __init__(self, query: aerospike.Query):
        self._query = query

    def add_ops(self, ops):
        '''Add a list of write ops to the query. When used with :meth:`Query.execute_background` the query will perform the write ops on any records found. If no predicate is attached to the Query it will apply ops to all the records in the specified set.
        '''
        return self._query.add_ops(ops)

    async def apply(self, module, function, arguments: List = None):
        '''Aggregate the results() using a stream UDF. If no predicate is attached to the Query the stream UDF will aggregate over all the records in the specified set.
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._query.apply, module, function, arguments))

    async def execute_background(self, policy: Dict = None) -> List:
        '''Buffer the records resulting from the query, and return them as a list of records.
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._query.execute_background, policy))

    def foreach(self, callback, policy: Dict = None):
        '''Invoke the callback function for each of the records streaming back from the query.
        '''
        return self._query.foreach(callback, policy)

    # async def get_parts(self) -> Dict:
    #     '''Gets the complete partition status of the query. Returns a dictionary of the form {id:(id, init, done, digest), ...}.
    #     '''
    #     loop = asyncio.get_event_loop()
    #     return await loop.run_in_executor(None,
    #                                       partial(self._query.get_parts))

    def is_done(self) -> bool:
        '''If using query pagination, did the previous paginated query with this query instance return all records?
        '''
        return self._query.is_done()
    
    @property
    def max_records(self):
        return self._query.max_records
    
    @max_records.setter
    def max_records(self, value:int):
        self._query.max_records = value
    
    def paginate(self):
        '''Set pagination filter to receive records in bunch (max_records or page_size).
        '''
        return self._query.paginate()

    @property
    def records_per_second(self):
        return self._query.records_per_second

    @records_per_second.setter
    def records_per_second(self, value:int):
        self._query.records_per_second = value



    async def results(self, policy: Dict = None) -> List:
        '''Buffer the records resulting from the query, and return them as a list of records.
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None,
                                          partial(self._query.results, policy))

    def select(self, *bins):
        '''Set a filter on the record bins resulting from results() or foreach(). If a selected bin does not exist in a record it will not appear in the bins portion of that record tuple.
        '''
        return self._query.select(*bins)

    @property
    def ttl(self):
        '''
        '''
        return self._query.ttl
    @ttl.setter
    def ttl(self, value:int):
        self._query.ttl = value

    def where(self, predicate, cdt_ctx=None):
        '''Set a where predicate for the query, without which the query will behave similar to aerospike.Scan. The predicate is produced by one of the aerospike.predicates methods equals() and between(). The list cdt_ctx is produced by one of the aerospike_helpers.cdt_ctx methods
        '''
        return self._query.where(predicate, cdt_ctx)
