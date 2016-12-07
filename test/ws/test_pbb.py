import sys
import requests
import json
from tools import json_rpc_header
from config import (
    username,
    pass_encrypted,)


#url = 'http://rpc.bogorkota.net:6543/ws/pbb'
url = 'http://localhost:6543/ws/pbb'
kode = '327771000100100030'
#kode = '611009000300201030'
#kode  = '327101000601300070'
tahun = '2016'
method = 'get_dop_bphtb'
#method = 'get_piutang_by_nop'
username = 'bphtb'
pass_encrypted = '$2a$10$cw31Np5qXwxR.SjijsAr4eAw2Eko7saayss7vkNUuo2uNHIdNzGMe'
#method = 'get_sppt_dop'
argv = sys.argv
if argv[1:]:
    method = argv[1]
if argv[2:]:
    kode = argv[2]
if argv[3:]:
    tahun = argv[3]
if argv[4:]:    
    username = argv[4]
    pass_encrypted = str(argv[6])

def get_dict(method,params):
    return dict(jsonrpc = '2.0',
                method = method,
                params = params,
                id = 1)

piutang = dict(
                kode  = kode,
                tahun  = tahun,
                count  = 10
            )


row_dicted = [] #[data1]
row_dicted.append(piutang)

headers = json_rpc_header(username, pass_encrypted)
params = dict(data=row_dicted)

print headers, pass_encrypted
#data = get_dict('get_sppt', params)
data = get_dict(method, params)

jsondata = json.dumps(data, ensure_ascii=False)
print('Send to {url}'.format(url=url))
print(jsondata)          
rows = requests.post(url, data=jsondata, headers=headers)
print('Result:')
print(json.loads(rows.text))
