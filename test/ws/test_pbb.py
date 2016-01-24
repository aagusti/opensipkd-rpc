import sys
import requests
import json
from tools import json_rpc_header
from config import (
    username,
    pass_encrypted,)


url = 'http://127.0.0.1:6543/ws/pbb'
#kode = '611010000300600440'
kode = '611009000300201030'
tahun = '2013'
#method = 'get_dop_bphtb'
method = 'get_piutang_by_nop'
argv = sys.argv
if argv[1:]:
    username = argv[1]
    pass_encrypted = str(argv[2])
if argv[3:]:
    kode = argv[3]
if argv[4:]:
        tahun = argv[4]

def get_dict(method,params):
    return dict(jsonrpc = '2.0',
                method = method,
                params = params,
                id = 1)

data1 = dict(
        kode    = kode,
        tahun  = tahun,)
data2 = dict(
        kode    = kode,
        )
data3 = dict(
        kode    = '6110100003',
        tahun  = '1998'
        )
        
#Rekap Desa By Kecamatan    
data4 = dict(
        kode    = '6110100', 
        tahun  = '1998'
        )
         
#Rekap Kecamatan    
data5 = dict(
        tahun  = '1998'
        )

piutang = dict(
                kode  = kode,
                tahun  = '2003',
                count  = 5
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
