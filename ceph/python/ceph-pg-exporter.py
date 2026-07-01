#!/usr/bin/env python3
from prometheus_client import start_http_server
from prometheus_client.core import InfoMetricFamily, GaugeMetricFamily, CounterMetricFamily, REGISTRY
import time, logging

from ceph.rados import PGs, Pools
from ceph.utils import ceph_stamp_to_datetime

LOG = logging.getLogger(__name__)
logging.basicConfig()
#LOG.level = logging.WARNING
LOG.level = logging.DEBUG

def log_timediff(msg, s):
    LOG.debug(f'{msg}: {(time.time_ns() - s)/1000000}')

class CephPGStatCollector(object):

    def __init__(self, rados):
        self._rados = rados
        self._pools = Pools(rados=rados)

    _pg_sum_keys = ['num_bytes', 'num_bytes_hit_set_archive', 'num_bytes_recovered', 'num_deep_scrub_errors', 'num_evict', 'num_evict_kb',
        'num_evict_mode_full', 'num_evict_mode_some', 'num_flush', 'num_flush_kb', 'num_flush_mode_high', 'num_flush_mode_low',
        'num_keys_recovered', 'num_large_omap_objects', 'num_legacy_snapsets', 'num_object_clones', 'num_object_copies', 'num_objects',
        'num_objects_degraded', 'num_objects_dirty', 'num_objects_hit_set_archive', 'num_objects_manifest', 'num_objects_misplaced',
        'num_objects_missing', 'num_objects_missing_on_primary', 'num_objects_omap', 'num_objects_pinned', 'num_objects_recovered',
        'num_objects_repaired', 'num_objects_unfound', 'num_omap_bytes', 'num_omap_keys', 'num_promote', 'num_read', 'num_read_kb',
        'num_scrub_errors', 'num_shallow_scrub_errors', 'num_whiteouts', 'num_write', 'num_write_kb',
    ]
    _pg_states = ['active', 'clean', 'scrubbing', 'deep', 'undersized', 'degraded', 'remapped', 'backfilling', 'inconsistent', 'creating',
        'activating', 'down', 'laggy', 'wait', 'peering', 'repair', 'recovering', 'forced_recovery', 'recovery_wait', 'recovery_toofull',
        'recovery_unfound', 'forced_backfill', 'backfill_wait', 'backfill_toofull', 'backfill_unfound', 'incomplete', 'stale', 'peered',
        'snaptrim', 'snaptrim_wait', 'snaptrim_error', 'unknown',
    ]
    _pg_gauges = {
        'ceph_pg_objects_scrubbed': {'desc': 'Number of scrubbed objects', 'key': 'objects_scrubbed'},
        'ceph_pg_last_scrub_duration': {'desc': 'Last scrub duration', 'key': 'last_scrub_duration', 'unit': 'seconds'},
        # scrub duration source unit is microseconds, convert to seconds
        'ceph_pg_scrub_duration': {'desc': 'Scrub duration', 'key': 'scrub_duration', 'unit': 'seconds', 'convert': lambda x: x/1000},
    }
    _pg_stamp_gauges = {
        'ceph_pg_last_fresh': {'desc': 'Timestamp of last fresh', 'key': 'last_fresh'},
        'ceph_pg_last_change': {'desc': 'Timestamp of last change', 'key': 'last_change'},
        'ceph_pg_last_active': {'desc': 'Timestamp of last active', 'key': 'last_active'},
        'ceph_pg_last_peered': {'desc': 'Timestamp of last peered', 'key': 'last_peered'},
        'ceph_pg_last_clean': {'desc': 'Timestamp of last clean', 'key': 'last_clean'},
        'ceph_pg_last_became_active': {'desc': 'Timestamp of last became active', 'key': 'last_became_active'},
        'ceph_pg_last_became_clean': {'desc': 'Timestamp of last became clean', 'key': 'last_became_peered'},
        'ceph_pg_last_unstale': {'desc': 'Timestamp of last unstale', 'key': 'last_unstale'},
        'ceph_pg_last_undegraded': {'desc': 'Timestamp of last undegraded', 'key': 'last_undegraded'},
        'ceph_pg_last_fullsized': {'desc': 'Timestamp of last fullsized', 'key': 'last_fullsized'},
        'ceph_pg_last_scrub_stamp': {'desc': 'Timestamp of last scrub', 'key': 'last_scrub_stamp'},
        'ceph_pg_last_deep_scrub_stamp': {'desc': 'Timestamp of last deep scrub', 'key': 'last_deep_scrub_stamp'},
        'ceph_pg_last_clean_scrub_stamp': {'desc': 'Timestamp of last clean scrub', 'key': 'last_clean_scrub_stamp'},
    }
    _pg_state_gauges = {}
    for state in _pg_states:
        desc = f'PG has state {state}'
        _pg_state_gauges[f'ceph_pg_state_{state}'] = { 'desc': desc, 'state': state}
    _pg_counters = {
        'ceph_pg_reported_seq': {'desc': 'Reported sequence number', 'key': 'reported_seq'},
        'ceph_pg_reported_epoch': {'desc': 'Reported epoch number', 'key': 'reported_epoch'},
        'ceph_pg_mapping_epoch' : {'desc': 'Mapping epoch number', 'key': 'mapping_epoch'},
        'ceph_pg_created': {'desc': 'Created epoch', 'key': 'created'},
        'ceph_pg_last_epoch_clean' : {'desc': 'Last clean epoch', 'key': 'last_epoch_clean'},
    }
    _pg_sum_counters = {}
    for key in _pg_sum_keys:
        desc = key.replace('_', ' ').title()
        _pg_sum_counters[f'ceph_pg_stat_sum_{key}'] = { 'desc': desc, 'key': key }

    def collect(self):
        s = st = time.time_ns()
        pgs = PGs(rados=self._rados)
        log_timediff('PGs object', s)
        pgInfo = InfoMetricFamily('ceph_pg', 'General PG info', labels = ['pgid'])
        s = time.time_ns()
        for pgstat in pgs.pg_stats:
            pgid = pgstat['pgid']
            pool_id = int(pgid.split('.')[0])
            pool_name = self._pools.pool_by_id(pool_id)['pool_name']
            pgInfo.add_metric([pgid], {
                'pool_id': str(pool_id),
                'pool_name': pool_name,
                'version': pgstat['version'],
            })
        yield pgInfo
        log_timediff('pgInfo', s)
        s = time.time_ns()
        for metric in self._pg_state_gauges:
            pgGauge = GaugeMetricFamily(metric, self._pg_state_gauges[metric]['desc'], labels=['pgid'])
            for pgstat in pgs.pg_stats:
                pg_states = pgstat['state'].split('+')
                have_state = 1 if self._pg_state_gauges[metric]['state'] in pg_states else 0
                pgGauge.add_metric([pgstat['pgid']], have_state)
            yield pgGauge
        log_timediff('State gauges', s)
        s = time.time_ns()
        for metric in self._pg_gauges:
            pgGauge = GaugeMetricFamily(metric, self._pg_gauges[metric]['desc'], labels=['pgid'], unit=self._pg_gauges[metric].get('unit', ''))
            for pgstat in pgs.pg_stats:
                pgGauge.add_metric([pgstat['pgid']], self._pg_gauges[metric].get('convert', lambda x: x)(pgstat[self._pg_gauges[metric]['key']]))
            yield pgGauge
        log_timediff('Gauges', s)
        s = time.time_ns()
        for metric in self._pg_stamp_gauges:
            pgGauge = GaugeMetricFamily(metric, self._pg_stamp_gauges[metric]['desc'], labels=['pgid'], unit='utctimestamp')
            for pgstat in pgs.pg_stats:
                pgGauge.add_metric([pgstat['pgid']], ceph_stamp_to_datetime(pgstat[self._pg_stamp_gauges[metric]['key']]).timestamp())
            yield pgGauge
        log_timediff('Stamp gauges', s)
        s = time.time_ns()
        for metric in self._pg_counters:
            pgCounter = CounterMetricFamily(metric, self._pg_counters[metric]['desc'], labels=['pgid'])
            for pgstat in pgs.pg_stats:
                pgCounter.add_metric([pgstat['pgid']], pgstat[self._pg_counters[metric]['key']])
            yield pgCounter
        log_timediff('Counters', s)
        s = time.time_ns()
        for metric in self._pg_sum_counters:
            
            pgCounter = CounterMetricFamily(metric, self._pg_sum_counters[metric]['desc'], labels=['pgid'])
            for pgstat in pgs.pg_stats:
                pgCounter.add_metric([pgstat['pgid']], pgstat['stat_sum'][self._pg_sum_counters[metric]['key']])
            yield pgCounter
        log_timediff('Sum counters', s)
        log_timediff('Collect total', st)

if __name__ == '__main__':
    cluster = PGs.rados_connect()
    start_http_server(9440)
    REGISTRY.register(CephPGStatCollector(cluster))
    while True:
        time.sleep(10)
