
import time
from sys import exit 
from uuid import uuid1

from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol
 
from cassandra import Cassandra
from cassandra.ttypes import NotFoundException, ColumnPath, \
    ColumnParent, SlicePredicate, SliceRange
from cassandra.ttypes import ConsistencyLevel as clevel

class cass_con_error(Exception):
    pass

def tuuid():
    return int(time.time()*1000)
def uuid():
    return uuid1().bytes

class cass_con:
    def __init__(self, keyspace, host='localhost', port=9160):
        self.socket = TSocket.TSocket(host, port)
        self.transport = TTransport.TBufferedTransport(self.socket)
        self.protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
        self.client = Cassandra.Client(self.protocol)
        self.host = host 
        self.port = port
        if keyspace == '':
            raise cass_con_error("invalid keyspace")
        self.keyspace = keyspace
        #make sure the database is alive
        self.transport.open()

    def __del__(self):
        self.transport.close()

    def insert(self, column_fam, key, col, 
               ts = None,
               const_level = clevel.ONE):

        for v in col.items():
            if type(v[1]) == type(str()):
                col_name, val = v
                cpath = ColumnPath(column_fam, column=col_name)
                if not ts:
                    ts = tuuid()
                try:
                    ret = self.client.insert(self.keyspace,
                                             key, cpath, val, ts, 
                                             const_level)
                except Exception:
                    raise
                ts = None
            elif type(v[1]) == type(dict()):
                scol, cols = v
                for t in cols.items():
                    col_name, val = t 
                    cpath = ColumnPath(column_fam, scol, col_name)
                    if not ts:
                        ts = tuuid()
                    try:
                        ret = self.client.insert(self.keyspace,
                                                 key, cpath, val, ts, 
                                                 const_level)
                    except Exception:
                        raise
                    ts = None
            else:
                raise cass_con_error("malformed input to insert: " +
                                     str(type(v[1])))
                

    def get_val(self, column_fam, key, col_name, 
                const_level = clevel.ONE):
        try:
            return self.client.get(self.keyspace, key,
                                   ColumnPath(column_fam, column=col_name),
                                   const_level).column.value
        except NotFoundException:
            return None

    #do I need to add reversed here?
    def get(self, col_fam, key,
            names = None,
            slice_range = ('', ''),
            const_level = clevel.ONE):
        start, finish = slice_range
        sr = SliceRange(start=start, finish=finish)
        val = self.client.get_slice(self.keyspace, key,
                                    ColumnParent(col_fam), 
                                    SlicePredicate(names,
                                                   sr),
                                    const_level)

        dct = {}
        for x in val:
            if x.column:
                dct[x.column.name] = x.column.value
            else:
                sdct = {}
                for c in x.super_column.columns:
                    sdct[c.name] = c.value
                dct[x.super_column.name] = sdct
        return dct
    
    def remove(self, col_fam, key, 
               col_name = None,
               super_col = None,
               const_level = clevel.ONE):
        try:
            self.client.remove(self.keyspace,
                               key,
                               ColumnPath(col_fam, 
                                          super_column=super_col,
                                          column=col_name),
                               tuuid(), 
                               const_level)

        except Exception:
            raise
