import sys
import requests
import json
from optparse import OptionParser
from sqlalchemy import engine_from_config
from pyramid.paster import get_appsettings
from tools import json_rpc_header
from ..models import (
    DBSession,
    User,
    )


DEFAULT_URL = 'http://localhost:6543/ws/pbb'
DEFAULT_NOP = '337102000700501570'
DEFAULT_TAHUN = '2014'
DEFAULT_USER = 'bphtb'

def get_dict(method,params):
    return dict(jsonrpc = '2.0',
                method = method,
                params = params,
                id = 1)

def main(argv=sys.argv):
    pars = OptionParser()
    pars.add_option('', '--url', default=DEFAULT_URL,
        help='default ' + DEFAULT_URL)
    pars.add_option('', '--nop', default=DEFAULT_NOP,
        help='default ' + DEFAULT_NOP)
    pars.add_option('', '--tahun', default=DEFAULT_TAHUN,
        help='default ' + DEFAULT_TAHUN)
    pars.add_option('', '--user', default=DEFAULT_USER,
        help='default ' + DEFAULT_USER)
    option, remain = pars.parse_args(argv[2:])
    config_uri = argv[1]
    url = option.url
    nop = option.nop
    tahun = option.tahun
    username = option.user
    settings = get_appsettings(config_uri)
    engine = engine_from_config(settings, 'sqlalchemy.')
    DBSession.configure(bind=engine)
    q = DBSession.query(User).filter_by(user_name=username)
    user = q.first()
    p = dict(kode=nop, tahun=tahun)
    rows = [p]
    headers = json_rpc_header(username, user.user_password)
    params = dict(data=rows)
    #print('{h} {p}'.format(h=headers, p=pass_encrypted))
    data = get_dict('get_dop_bphtb', params)
    jsondata = json.dumps(data, ensure_ascii=False)
    print('Send to {url}'.format(url=url))
    print(jsondata)          
    rows = requests.post(url, data=jsondata, headers=headers)
    print('Result:')
    print(json.loads(rows.text))


