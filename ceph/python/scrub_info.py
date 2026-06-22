#!/usr/bin/env python3
# scrub_report function based on https://github.com/frans42/ceph-goodies/blob/main/scripts/pool-scrub-report
import json, re
from datetime import datetime, timedelta
from collections import namedtuple
from ceph.rados import PGs, CephConf, Pools
from math import ceil

scrub_interval_period = 6 * 3600
deep_scrub_interval_period = 24 * 3600

def log_stderr(msg):
    print(msg, file=stderr)

def log_stderr_and_exit(msg, rc):
    log_stderr(msg)
    exit(rc)

config_options = {
        'osd_deep_scrub_interval': float,
        'osd_scrub_min_interval': float,
        'osd_scrub_max_interval': float,
        'osd_scrub_interval_randomize_ratio': float,
        'osd_deep_scrub_randomize_ratio': float,
        'osd_max_scrubs': int,
}

dformat = '%Y-%m-%dT%H:%M:%S.%f%z'
def get_stamp(data):
    return datetime.strptime(data, dformat)

def get_states(data):
    return data.split('+')

ScrubSchedule = namedtuple('ScrubSchedule', ['deep', 'date'])
re_periodic = re.compile(r'^periodic (?P<deep>deep )?scrub scheduled @ (?P<stamp>.*)$')
re_running = re.compile(r'^(?P<deep>deep )?scrubbing for (?P<time>[0-9]+)s$')

def get_scheduled(data):
    m = re_periodic.match(data)
    if m:
        return ScrubSchedule((m['deep']) is not None, get_stamp(m['stamp']))
    m = re_running.match(data)
    if m:
        return ScrubSchedule((m['deep'] is not None), timedelta(seconds=int(m['time'])))
    return data

class ScrubReportCounters:
    def __init__(self):
        self.count = 0
        self.pgs_scrubbing = 0
        self.pgs_deep_scrubbing = 0
        self.unclean = 0
        self.pg_list = set()
    def __repr__(self):
        return f'({self.count},{self.pgs_scrubbing},{self.pgs_deep_scrubbing},{self.unclean},{self.pg_list})'
    def add(self, other):
        self.count += other.count
        self.pgs_scrubbing += other.pgs_scrubbing
        self.pgs_deep_scrubbing += other.pgs_deep_scrubbing
        self.unclean += other.unclean
        self.pg_list |= other.pg_list

def scrub_report(pgs, cfg, pool_id=None):
    now = datetime.now().astimezone()
    total_scrub_diff = total_deep_scrub_diff = 0
    total_scrub_over = total_deep_scrub_over = 0
    ls_max_interval = lds_max_interval = 0
    scrub_hist = [ScrubReportCounters()]
    deep_scrub_hist = [ScrubReportCounters()]
    pg_osds = {}
    tnpgs = 0
    osd_busy = set()
    for pg_stat in pgs.pg_stats:
        last_scrub = get_stamp(pg_stat['last_scrub_stamp'])
        last_deep_scrub = get_stamp(pg_stat['last_deep_scrub_stamp'])
        #scrub_schedule = get_scheduled(pg_stat['scrub_schedule'])
        states = get_states(pg_stat['state'])
        is_in_pool = pool_id and pg_stat['pgid'].startswith(f'{pool_id}.')
        if is_in_pool:
            ls_diff = (now - last_scrub).total_seconds()
            if ls_diff > cfg.osd_scrub_min_interval:
                total_scrub_diff += ls_diff
                total_scrub_over += 1
            ls_interval = int(ls_diff / scrub_interval_period)
            while len(scrub_hist) <= ls_interval:
                scrub_hist.append(ScrubReportCounters())
            scrub_hist[ls_interval].count += 1

            lds_diff = (now - last_deep_scrub).total_seconds()
            if lds_diff > cfg.osd_deep_scrub_interval:
                total_deep_scrub_diff += lds_diff
                total_deep_scrub_over += 1
            lds_interval = int(lds_diff / deep_scrub_interval_period)
            while len(deep_scrub_hist) <= lds_interval:
                deep_scrub_hist.append(ScrubReportCounters())
            deep_scrub_hist[lds_interval].count += 1
            tnpgs += 1

        is_busy = False
        if 'scrubbing' in states:
            if is_in_pool:
                if 'deep' in states:
                    scrub_hist[ls_interval].pgs_deep_scrubbing += 1
                    deep_scrub_hist[ls_interval].pgs_deep_scrubbing += 1
                else:
                    scrub_hist[ls_interval].pgs_scrubbing += 1
                    deep_scrub_hist[ls_interval].pgs_scrubbing += 1
            is_busy = True
        elif 'active' in states and 'clean' in states:
            if is_in_pool:
                scrub_hist[ls_interval].pg_list.add(pg_stat['pgid'])
                deep_scrub_hist[ls_interval].pg_list.add(pg_stat['pgid'])
        elif cfg.osd_scrub_during_recovery and 'active' in states and 'remapped' in states:
            if is_in_pool:
                scrub_hist[ls_interval].pg_list.add(pg_stat['pgid'])
                deep_scrub_hist[ls_interval].pg_list.add(pg_stat['pgid'])
            is_busy = True
        else:
            if is_in_pool:
                scrub_hist[ls_interval].unclean += 1
                deep_scrub_hist[ls_interval].unclean += 1
            is_busy = True
        if is_busy:
            osd_busy |= set(pg_stat['acting'])
        if not pg_stat['pgid'] in pg_osds:
            pg_osds[pg_stat['pgid']] = set()
        pg_osds[pg_stat['pgid']] = pg_stat['acting']

    print('Scrub report:')
    cnpgs = ScrubReportCounters()
    npgse = 0
    nidle = 0
    for i, ls_counters in enumerate(scrub_hist):
        if ls_counters.count == 0:
            continue
        cnpgs.add(ls_counters)
        print('{:5d}%{:7d} PGs not scrubbed since {:2d} intervals ({:3d}h)'.format(int(100 * cnpgs.count / tnpgs), ls_counters.count, i + 1, (i + 1) * int(scrub_interval_period / 3600)), end='')
        busy_pg = 0
        if (i + 1) * scrub_interval_period > cfg.osd_scrub_min_interval:
            npgse += ls_counters.count
            for pg in ls_counters.pg_list:
                for o in pg_osds[pg]:
                     if o in osd_busy:
                        busy_pg += 1
                        break
            idle = len(ls_counters.pg_list) - busy_pg
            if idle:
                nidle += idle
                print(f' [{idle} idle]', end='')
        ls_counters.pgs_deep_scrubbing > 0 and print(f' [{ls_counters.pgs_deep_scrubbing} deep scrubbing]', end='')
        ls_counters.unclean > 0 and printf(f' [{ls_counters.unclean} unclean]', end='')
        ls_counters.pgs_scrubbing > 0 and printf(f' [{ls_counters.pgs_scrubbing} scrubbing]', end='')
        print()
    est = total_scrub_diff / total_scrub_over / 86400 if total_scrub_over > 0 else cfg.osd_scrub_min_interval / scrub_interval_period / 3600
    estc = cfg.osd_scrub_min_interval * (1 + cfg.osd_scrub_interval_randomize_ratio / 3) / 86400
    nidlec = (nidle / npgse if npgse > 0 else 1) * 100
    print('      {:7d} PGs,  EST={:.2f}d ({:.2f}d), {:d} scrubbing, {:d} ({:1.2f}%) idle, {:d} unclean'.format(
        tnpgs, est, estc, cnpgs.pgs_scrubbing, nidle, nidlec, cnpgs.unclean))

    print('\nDeep scrub report:')
    cnpgs = ScrubReportCounters()
    for i, lds_counters in enumerate(deep_scrub_hist):
        if lds_counters.count == 0:
            continue
        cnpgs.add(lds_counters)
        print('{:5d}%{:7d} PGs not deep-scrubbed since {:2d} intervals ({:3d}h)'.format(
            int(100 * cnpgs.count / tnpgs), lds_counters.count, i + 1, (i + 1) * int(deep_scrub_interval_period / 3600)))
    est = total_deep_scrub_diff / total_deep_scrub_over / 86400 if total_deep_scrub_over > 0 else cfg.osd_deep_scrub_interval / deep_scrub_interval_period
    print('      {:7d} PGs, EDST={:.2f}d, {:d} deep-scrubbing, {:d} unclean'.format(tnpgs, est, cnpgs.pgs_deep_scrubbing, cnpgs.unclean))
    print()

    if pool_id:
        pools = Pools()
        p = pools.pool_by_id(pool_id)
        smi_eq_occup = ceil(100 / (1 + .5 * cfg.osd_scrub_interval_randomize_ratio))
        spgs_per_bucket = smi_eq_occup * p['pg_num'] * (scrub_interval_period / 3600) / (cfg.osd_scrub_min_interval / 36)
        dsmi_eq_occup = cfg.osd_deep_scrub_interval / 36 / (cfg.osd_deep_scrub_interval / 3600 + .5 * 
            (cfg.osd_scrub_min_interval / 3600 * (1 + .5 * cfg.osd_scrub_interval_randomize_ratio)))
        dspgs_per_bucket = dsmi_eq_occup * p['pg_num'] * (deep_scrub_interval_period / 3600) / (cfg.osd_deep_scrub_interval / 36)
    print('Configuration values used by the reports:')
    print('scrub_min_interval={:.1f}h ({:.1f}d/{:.1f}i/{:d}%/{:.2f}PGs÷i)'.format(
            cfg.osd_scrub_min_interval / 3600,
            cfg.osd_scrub_min_interval / 86400,
            cfg.osd_scrub_min_interval / scrub_interval_period,
            smi_eq_occup, spgs_per_bucket))
    print(f'scrub_max_interval={cfg.osd_scrub_max_interval/3600}h ({cfg.osd_scrub_max_interval/86400}d)')
    print('deep_scrub_interval={:.1f}h ({:.1f}d/~{:.2f}%/~{:.2f}PGS÷d)'.format(
        cfg.osd_deep_scrub_interval / 3600,
        cfg.osd_deep_scrub_interval / 86400,
        dsmi_eq_occup,
        dspgs_per_bucket
        ))
    print(f'osd_scrub_interval_randomize_ratio={cfg.osd_scrub_interval_randomize_ratio}')
    print(f'osd_deep_scrub_randomize_ratio={cfg.osd_deep_scrub_randomize_ratio}')
    print(f'osd_max_scrubs={cfg.osd_max_scrubs}')
    print(f'osd_scrub_backoff_ratio={cfg.osd_scrub_backoff_ratio}')
    print(f'mon_warn_pg_not_scrubbed_ratio={cfg.mon_warn_pg_not_scrubbed_ratio}')
    print(f'mon_warn_pg_not_deep_scrubbed_ratio={cfg.mon_warn_pg_not_deep_scrubbed_ratio}')

def main():
    #pg = '30.1f0'
    pool = 'cephfs.experiments.data'
    cluster = PGs.rados_connect()
    cfg = CephConf(rados=cluster)
    pgs = PGs(rados=cluster)
    scrub_report(pgs, cfg, 31)
    exit(0)
    cmd = {'prefix': 'pg', 'pgid': pg, 'cmd': 'query'}
    (ret, outbuf, outs) = cluster.pg_command(pg, json.dumps(cmd), b'')
    resp = json.loads(outbuf)
    stats = resp['info']['stats']
    last_scrub = datetime.strptime(stats['last_scrub_stamp'], dformat)
    last_deep_scrub = datetime.strptime(stats['last_deep_scrub_stamp'], dformat)
    next_scrub = datetime.strptime(resp['scrubber']['scrub_reg_stamp'], dformat)
    osd_scrub_max_sched = osd_scrub_min_interval * (1 + osd_scrub_interval_randomize_ratio)
    min_next_scrub = last_scrub + timedelta(seconds=osd_scrub_min_interval)
    max_next_scrub = last_scrub + timedelta(seconds=osd_scrub_max_sched)
    print(f' scrub  last: {last_scrub}\n scrub  next: {next_scrub}\n scrub  diff: {next_scrub - last_scrub}')
    print(f' scrub range: {min_next_scrub} - {max_next_scrub}, in range: {min_next_scrub <= next_scrub and next_scrub <= max_next_scrub}')
    print(f' scrub force: {last_scrub + timedelta(seconds=osd_scrub_max_interval)}')
    print(f'dscrub last: {last_deep_scrub}\ndscrub next: {next_scrub}\ndscrub diff: {next_scrub - last_deep_scrub}\ndscrub  max: {last_deep_scrub + timedelta(seconds=osd_deep_scrub_interval)}')

if __name__ == '__main__':
    main()
