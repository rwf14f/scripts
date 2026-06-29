from datetime import datetime

def ceph_stamp_to_datetime(stamp_str):
    return datetime.strptime(stamp_str, '%Y-%m-%dT%H:%M:%S.%f%z')