import aerospike
import asyncio
from functools import partial
from typing import List, Dict


class AioAerospikeScan():
    def __init__(self, scan:aerospike.Scan) -> None:
        self._scan = scan

    def add_ops(self, ops):
        '''Add a list of write ops to the query. When used with :meth:`Query.execute_background` the query will perform the write ops on any records found. If no predicate is attached to the Query it will apply ops to all the records in the specified set.
        '''
        return self._scan.add_ops(ops)    
    async def results(self, policy=None , nodename=None) -> List:
        '''Buffer the records resulting from the scan, and return them as a list of records.If provided nodename should be the Node ID of a node to limit the scan to.
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, 
                partial(self._scan.results, policy , nodename))
            
    async def results(self,policy=None , nodename=None) -> List:
        '''Buffer the records resulting from the scan, and return them as a list of records.If provided nodename should be the Node ID of a node to limit the scan to.
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, 
                partial(self._scan.results, policy , nodename))
            
    async def results(self,policy=None , nodename=None) -> List:
        '''Buffer the records resulting from the scan, and return them as a list of records.If provided nodename should be the Node ID of a node to limit the scan to.
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, 
                partial(self._scan.results, policy , nodename))
            
    async def foreach(self, callback=None, policy=None, options=None, nodename=None):
        '''Invoke the callback function for each of the records streaming back from the scan. If provided nodename should be the Node ID of a node to limit the scan to.
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, 
                partial(self._scan.foreach, callback, policy, options , nodename))
            
    # async def get_parts(self) -> Dict:
    #     '''Gets the complete partition status of the scan. Returns a dictionary of the form {id:(id, init, done, digest), ...}.
    #     '''
    #     loop = asyncio.get_event_loop()
    #     return await loop.run_in_executor(None, 
    #             partial(self._scan.get_parts ))
            
    def is_done(self) -> bool:
        '''Gets the status of scan
        '''
        return self._scan.is_done()
            
    def paginate(self):
        '''Set pagination filter to receive records in bunch (max_records or page_size).
        '''
        return self._scan.paginate()
            
    async def results(self, policy =None , nodename=None) -> List:
        '''Buffer the records resulting from the scan, and return them as a list of records.If provided nodename should be the Node ID of a node to limit the scan to.
        '''
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, 
                partial(self._scan.results, policy , nodename))
            
    def select(self, *bins):
        '''Set a filter on the record bins resulting from results() or foreach(). If a selected bin does not exist in a record it will not appear in the bins portion of that record tuple.
        '''
        return self._scan.select(*bins)
            