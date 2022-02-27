from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
import uuid

'''
USERNAME@cqlsh:thingsboard> DESCRIBE  ts_kv_cf ;

CREATE TABLE thingsboard.ts_kv_cf (
    entity_type text,
    entity_id timeuuid,
    key text,
    partition bigint,
    ts bigint,
    bool_v boolean,
    dbl_v double,
    long_v bigint,
    str_v text,
    PRIMARY KEY ((entity_type, entity_id, key, partition), ts)
) WITH CLUSTERING ORDER BY (ts ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';
'''

USERNAME = 'USERNAME'
PASSWORD = 'PASSWORD'
HOST = 'HOST'
PORT = 9042
KEYSPACE = 'thingsboard'
ENTITY_TYPE = 'DEVICE'
ENTITY_ID = uuid.UUID('5a33aef0-78d0-11ec-98b6-857d4b84eb04')
KEY = 'key1'
PARTITION = 1643673600 * 1000
TS_START = 1645718400 * 1000
TS_END = 1645977600 * 1000

auth_provider = PlainTextAuthProvider(username=USERNAME, password=PASSWORD)
cluster = Cluster([HOST], port=PORT, auth_provider=auth_provider)
session = cluster.connect(KEYSPACE)

query = SimpleStatement(
    "SELECT * FROM ts_kv_cf WHERE entity_type = %s AND entity_id = %s AND key = %s AND partition = %s AND ts >= %s AND ts < %s",
    consistency_level=ConsistencyLevel.QUORUM)
rows = session.execute(query, (ENTITY_TYPE, ENTITY_ID, KEY, PARTITION, TS_START, TS_END))
for row in rows:
    print(row)
