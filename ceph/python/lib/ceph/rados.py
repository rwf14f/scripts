import rados, re, json

re_replace_inf = re.compile(r'((?:"\s*:|[,[])\s*-?)inf(\W)')
nolfcr = str.maketrans('', '', '\n\r')

def fix_json_constants(json_str):
    # replace all occurrences of the constants 'inf' with 'Infinity' and '-inf' with '-Infinity'
    # to make it a bit more robust, the source constant has to be preceded either by
    # "\s*:\s* (dict key), or [\s* or ,\s* (array element)
    # none of those combinations of characters should appear in the json that we're parsing here
    return re_replace_inf.sub(r'\1Infinity\2', json_str.translate(nolfcr))

class RadosMixin:
    @staticmethod
    def rados_connect(conffile=''):
        cluster = rados.Rados(conffile=conffile)
        cluster.connect()
        return cluster

    def __init__(self, rados=None, **kwargs):
        self._rados_object = rados
        self._ioctx_object = None

    @property
    def _rados(self):
        if self._rados_object is None:
            self._rados_object = RadosMixin.rados_connect()
        return self._rados_object
    
    @property
    def _ioctx(self):
        if self._ioctx_object is None:
            self._ioctx_object = self._rados.open_ioctx2(self.pool_id)
        return self._ioctx_object

class PGs(RadosMixin):
    _pg_dump_cmd = {'prefix': 'pg dump', 'dumpcontents': ['pgs'], 'target': ('mon-mgr', ''), 'format': 'json'}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._pg_dump = None

    def update_pg_dump(self):
        (ret, outbuf, outs) = self._rados.mgr_command(json.dumps(self._pg_dump_cmd), b'')
        if not ret:
            self._pg_dump = json.loads(outbuf)
        return (not ret)

    @property
    def pg_dump(self):
        if self._pg_dump is None:
            if not self.update_pg_dump():
                return {}
        return self._pg_dump

    @property
    def pg_stats(self):
        if 'pg_stats' in self.pg_dump:
            for pg in self.pg_dump['pg_stats']:
                yield pg

class Pools(RadosMixin):
    _pool_info_cmd = {'prefix': 'osd pool ls', 'detail': 'detail', 'format': 'json'}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._pool_info = None
        self._pool_by_names = {}
        self._pool_by_ids = {}

    def _update_pool_info(self):
        (ret, outbuf, outs) = self._rados.mon_command(json.dumps(self._pool_info_cmd), b'', target='')
        if not ret:
            try:
                self._pool_info = json.loads(outbuf.decode('utf-8'))
            except json.JSONDecodeError:
                # a decode error is most likely caused by the json containing an 'inf' constant
                # which the json decoder can't handle (it only allows Infinity)
                # so try to convert 'inf' to 'Infinity' and try to reparse it
                # this is somewhat fragile, but there's no hook in the python json parsing
                # that would allow to work around it properly
                # it should be sufficiently robust to parse the pool list json
                self._pool_info = json.loads(fix_json_constants(outbuf.decode('utf-8')))
        return (not ret)

    @property
    def pools(self):
        if self._pool_info is None:
            if not self._update_pool_info():
                return {}
        for pool in self._pool_info:
            yield pool

    def pool_by_id(self, pool_id):
        if pool_id in self._pool_by_ids:
            return self._pool_by_ids[pool_id]
        for p in self.pools:
            if p['pool_id'] == pool_id:
                self._pool_by_ids[pool_id] = p
                return p
        return None
    
    def pool_by_name(self, name):
        if name in self._pool_by_names:
            return self._pool_by_names[name]
        for p in self.pools:
            if p['pool_name'] == name:
                self._pool_by_names[name] = p
                return p
        return None

class CephConf(RadosMixin):
    _re_int = re.compile(r'^[0-9]+$')
    _re_float = re.compile(r'^([0-9]+\.|\.[0-9]+|[0-9]+\.[0-9]+)$')

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @classmethod
    def convert(cls, value):
        if cls._re_int.match(value):
            return int(value)
        if cls._re_float.match(value):
            return float(value)
        return str(value)

    def _get_mon_config_entry(self, attr):
        cmd = {'prefix': 'config get', 'who': 'mon', 'key': attr}
        (ret, outbuf, outs) = self._rados.mon_command(json.dumps(cmd), b'', target='')
        if ret:
            return cluster.conf_get(opt).strip()
        return outbuf.decode('utf-8').strip()
    
    def __getattribute__(self, attr):
        try:
            return super().__getattribute__(attr)
        except AttributeError as ae:
            val = CephConf.convert(self._get_mon_config_entry(attr))
            setattr(CephConf, attr, property(lambda obj: val))
            # the code below is better suited if modification of config options is needed
#            setattr(self, f'_{attr}', val)
#            setattr(CephConf, attr, property(lambda obj: getattr(obj, f'_{attr}')))
        return super().__getattribute__(attr)
