from ..ws import (auth_from_rpc, LIMIT, CODE_OK, CODE_NOT_FOUND, CODE_DATA_INVALID, 
                   CODE_INVALID_LOGIN, CODE_NETWORK_ERROR)
from pyramid_rpc.jsonrpc import jsonrpc_method
from ...models import pbb_DBSession
from ...models.pbb import Sppt, DatObjekPajak, DatOpBumi
#, PembayaranSppt
from ...tools import FixLength
from datetime import datetime


@jsonrpc_method(method='get_sppt_bayar', endpoint='ws_pbb')
def get_sppt_bayar(request, data):
    # Digunakan untuk menghitung piutang berdasarkan nop dan tahun selama periode tertentu
    # paramter input nop tahun akhir jumlah tahun yang dihitung
    resp, user = auth_from_rpc(request)
    if resp['code'] != 0:
        return resp
    # try:
    if 1 == 1:
        ret_data = []
        for r in data:
            query = Sppt.get_bayar(r['kode'])
            p_count = 'count' in r and r['count'] or 0
            p_tahun = 'tahun' in r and r['tahun'] or datetime.now().strftime('%Y')
            if int(p_count) and int(p_tahun):
                p_tahun_awal = str(int(p_tahun)-int(p_count)+1)
                query = query.filter(Sppt.thn_pajak_sppt.between(p_tahun_awal,p_tahun))
            elif p_tahun:
                query.filter(Sppt.thn_pajak_sppt==p_tahun)
            query = query.order_by(Sppt.thn_pajak_sppt.desc())
            row = query.first()
            if not row:
                resp['code'] = CODE_NOT_FOUND
                resp['message'] = 'DATA TIDAK DITEMUKAN'
                return resp
            fields = row._fields
            rows = query.all()
            if rows:
                for row in rows:
                    ret_data.append(dict(zip(fields, row)))
    # except:
    # return dict(code = CODE_DATA_INVALID, message = 'Data Invalid')

    params = dict(data=ret_data)
    return dict(code=CODE_OK, message='Data Submitted', params=params)

@jsonrpc_method(method='get_info_op', endpoint='ws_pbb')
def get_info_op(request, data):
    #Digunakan untuk generator info nop
    #parameter kode, [tahun]
    #Contoh Parameter
    #Memperoleh Nop Tertentu            nop, tahun
    #Memperoleh Daftar Nop              nop
    
    resp,user = auth_from_rpc(request)
    if resp['code'] != 0:
        return resp
    ret_data = []
    for r in data:
        query = Sppt.get_info_op(r['kode'])
        if 'tahun' in r and r['tahun']:
            query = query.filter(Sppt.thn_pajak_sppt==r['tahun'])
        row  =  query.first()
        if not row:
            resp['code'] = CODE_NOT_FOUND 
            resp['message'] = 'DATA TIDAK DITEMUKAN'
            return resp
        fields = row._fields
        rows = query.all()
        if rows:
            for row in rows:
                ret_data.append(dict(zip(fields,row)))
    params = dict(data=ret_data)
    return dict(code = CODE_OK, message = 'Data Submitted',params = params)
    
@jsonrpc_method(method='get_dop_bphtb', endpoint='ws_pbb')
def get_dop_bphtb(request, data):
    #Digunakan untuk info nop pbb
    #parameter kode, [tahun]
    #Contoh Parameter
    #Memperoleh Nop Tertentu            nop, tahun
    #Memperoleh Daftar Nop              nop
    list_nop = "321807101000122490"
    resp,user = auth_from_rpc(request)
    if resp['code'] != 0:
        return resp
    ret_data =[]
    for r in data:
        dob = DatOpBumi.get_by_nop(r['kode']).first()
        if dob.jns_bumi > '3':
            resp['code'] = CODE_NOT_FOUND
            resp['message'] = 'NOP TIDAK AKTIF, SILAHKAN HUBUNGI BIDANG PBB'
            return resp
        if Sppt.count(r['kode'])>0:
            query = Sppt.get_info_op_bphtb(r['kode'],r['tahun'])
        if r['kode']==list_nop:
             query = DatObjekPajak.get_info_op_bphtb(r['kode'])
        else:
            #iWan Mampir, Req Pemda 20250116 - Hit ke SPPT sementara
            #query = DatObjekPajak.get_info_op_bphtb(r['kode'])
            query = Sppt.get_info_op_bphtb(r['kode'],r['tahun'])
        row  =  query.first()
        if not row:
            resp['code'] = CODE_NOT_FOUND 
            resp['message'] = 'DATA TIDAK DITEMUKAN'
            return resp
        fields = row._fields
        rows = query.all()
        if rows:
            for row in rows:
                ret_data.append(dict(zip(fields,row)))
    params = dict(data=ret_data)
    return dict(code = CODE_OK, message = 'Data Submitted',params = params)    

@jsonrpc_method(method='get_piutang_by_nop', endpoint='ws_pbb')
def get_piutang_by_nop(request, data):
    #Digunakan untuk menghitung piutang berdasarkan nop dan tahun selama periode tertentu
    # paramter input nop tahun akhir jumlah tahun yang dihitung
    resp,user = auth_from_rpc(request)
    if resp['code'] != 0:
        return resp
    #try:
    if 1==1:
        ret_data =[]
        for r in data:
            dob = DatOpBumi.get_by_nop(r['kode']).first()
            if dob.jns_bumi > '3':
                resp['code'] = CODE_NOT_FOUND
                resp['message'] = 'NOP TIDAK AKTIF, SILAHKAN HUBUNGI BIDANG PBB'
                return resp
            query = Sppt.get_piutang(r['kode'],r['tahun'],r['count'])
            row  =  query.first()
            if not row:
                resp['code'] = CODE_NOT_FOUND 
                resp['message'] = 'DATA TIDAK DITEMUKAN'
                return resp

            fields = row._fields
            rows = query.all()
            if rows:
                for row in rows:
                    ret_data.append(dict(zip(fields,row)))
    #except:
    #    return dict(code = CODE_DATA_INVALID, message = 'Data Invalid')
    
    params = dict(data=ret_data)
    return dict(code = CODE_OK, message = 'Data Submitted',params = params)    
    
@jsonrpc_method(method='get_sppt_dop', endpoint='ws_pbb')
def get_sppt_dop(request, data):
    #Digunakan untuk menghitung piutang berdasarkan nop dan tahun selama periode tertentu
    # paramter input nop tahun akhir jumlah tahun yang dihitung
    resp,user = auth_from_rpc(request)
    if resp['code'] != 0:
        return resp
    # try:
    if 1==1:
        ret_data =[]
        for r in data:
            query = Sppt.get_dop(r['kode'],r['tahun'])
            row  =  query.first()
            if not row:
                resp['code'] = CODE_NOT_FOUND 
                resp['message'] = 'DATA TIDAK DITEMUKAN'
                return resp
            fields = row._fields
            rows = query.all()
            if rows:
                for row in rows:
                    ret_data.append(dict(zip(fields,row)))
    # except:
        # return dict(code = CODE_DATA_INVALID, message = 'Data Invalid')
    
    params = dict(data=ret_data)
    return dict(code = CODE_OK, message = 'Data Submitted',params = params)    
    

@jsonrpc_method(method='get_sppt', endpoint='ws_pbb')
def get_sppt(request, data):
    #Digunakan untuk generator sppt per nop
    #parameter kode, [tahun]
    #Contoh Parameter
    #Memperoleh Nop Tertentu            nop, tahun
    #Memperoleh Daftar Nop              nop
    #Memperoleh Daftar Nop Perdesa      kd_desa, tahun
    #Memperoleh Daftar Nop Perkelurahan kd_kelurahan, tahun
    
    resp,user = auth_from_rpc(request)
    if resp['code'] != 0:
        return resp
    try:
    #if 1==1:
        ret_data =[]
        for r in data:
          
            if len(r['kode'])==7: #kode= 7 digit berarti nop per kecamatan
                rows = Sppt.get_nop_by_kecamatan(r['kode'],r['tahun']).all()
            if len(r['kode'])==10: #kode= 10 digit berarti nop per desa
                rows = Sppt.get_nop_by_kelurahan(r['kode'],r['tahun']).all()
            else:
                if 'tahun' in r and r['tahun']:
                    rows = Sppt.get_nop_by_nop_thn(r['kode'],r['tahun']).all()
                else:
                    rows = Sppt.get_by_nop(r['kode']).all()
            if rows:
                for row in rows:
                    ret_data.append(row.to_dict())
    except:
        return dict(code = CODE_DATA_INVALID, message = 'Data Invalid')
    
    params = dict(data=ret_data)
    return dict(code = CODE_OK, message = 'Data Submitted',params = params)

@jsonrpc_method(method='get_sppt_rekap_desa', endpoint='ws_pbb')
def get_sppt_rekap_desa(request, data):
    resp,user = auth_from_rpc(request)
    if resp['code'] != 0:
        return resp
    #try:
    if 1==1:
        ret_data =[]
        for r in data:
            query = Sppt.get_rekap_desa(r['kode'],r['tahun'])
            fields = query.first().keys()
            rows = query.all()
            if rows:
                for row in rows:
                    ret_data.append(dict(zip(fields,row)))
    #except:
    #    return dict(code = CODE_DATA_INVALID, message = 'Data Invalid')
    
    params = dict(data=ret_data)
    return dict(code = CODE_OK, message = 'Data Submitted',params = params)
    
@jsonrpc_method(method='get_sppt_rekap_kecamatan', endpoint='ws_pbb')
def get_sppt_rekap_kecamatan(request, data):
    resp,user = auth_from_rpc(request)
    if resp['code'] != 0:
        return resp
    #try:
    if 1==1:
        ret_data =[]
        for r in data:
            query = Sppt.get_rekap_kec(r['tahun'])
            fields = query.first().keys()
            rows = query.all()
            if rows:
                for row in rows:
                    ret_data.append(dict(zip(fields,row)))
    #except:
    #    return dict(code = CODE_DATA_INVALID, message = 'Data Invalid')
    
    params = dict(data=ret_data)
    return dict(code = CODE_OK, message = 'Data Submitted',params = params)
    
@jsonrpc_method(method='get_dop', endpoint='ws_pbb')
def get_dop(request, data):
    #Digunakan untuk generator info nop
    #parameter kode, [tahun]
    #Contoh Parameter
    #Memperoleh Nop Tertentu            nop, tahun
    #Memperoleh Daftar Nop              nop
    resp,user = auth_from_rpc(request)
    if resp['code'] != 0:
        return resp
    try:
    #if 1==1:
        ret_data =[]
        for r in data:
          
            if len(r['kode'])==7: #kode= 7 digit berarti nop per kecamatan
                rows = DatObjekPajak.get_by_kecamatan(r['kode']).all()
            if len(r['kode'])==10: #kode= 10 digit berarti nop per desa
                rows = DatObjekPajak.get_by_kelurahan(r['kode']).all()
            else:
                rows = DatObjekPajak.get_by_nop(r['kode']).all()
            if rows:
                for row in rows:
                    ret_data.append(row.to_dict())
    except:
        return dict(code = CODE_DATA_INVALID, message = 'Data Invalid')
    
    params = dict(data=ret_data)
    return dict(code = CODE_OK, message = 'Data Submitted',params = params)

@jsonrpc_method(method='get_transaksi', endpoint='ws_pbb')
def get_transaksi(request, data):
    #Digunakan untuk mendapatkan data ketetapan dan pembayaran
    #parameter kode

    resp,user = auth_from_rpc(request)
    if resp['code'] != 0:
        return resp
    #try:
    if 1==1:
        ret_data =[]
        for r in data:
            query = Sppt.get_transaksi(r['kode'])
            row  =  query.first()
            if not row:
                resp['code'] = CODE_NOT_FOUND
                resp['message'] = 'DATA TIDAK DITEMUKAN'
                return resp

            # fields = row.keys()
            fields = row._fields
            rows = query.all()
            if rows:
                for row in rows:
                    ret_data.append(dict(zip(fields,row)))
    #except:
    #    return dict(code = CODE_DATA_INVALID, message = 'Data Invalid')

    params = dict(data=ret_data)
    return dict(code = CODE_OK, message = 'Data Submitted',params = params)

@jsonrpc_method(method='get_dop_only_bphtb', endpoint='ws_pbb')
def get_dop_only_bphtb(request, data):
    #Digunakan untuk info nop pbb
    #parameter kode, [tahun]
    #Contoh Parameter
    #Memperoleh Nop Tertentu            nop, tahun
    #Memperoleh Daftar Nop              nop
    resp,user = auth_from_rpc(request)
    if resp['code'] != 0:
        return resp
    try:
        ret_data =[]
        for r in data:
            #query = DatObjekPajak.get_info_op_bphtb(r['kode'])
            #row  =  query.first()
            #if not row:
                #resp['code'] = CODE_NOT_FOUND
                #resp['message'] = 'DATA TIDAK DITEMUKAN'
                #return resp
            #fields = row.keys()
            #rows = query.all()

            if len(r['kode'])==7: #kode= 7 digit berarti nop per kecamatan
                rows = DatObjekPajak.get_info_op_bphtb(r['kode'])
            if len(r['kode'])==10: #kode= 10 digit berarti nop per desa
                rows = DatObjekPajak.get_info_op_bphtb(r['kode'])
            else:
                rows = DatObjekPajak.get_info_op_bphtb(r['kode'])

            fields = row._fields

            if rows:
                for row in rows:
                    ret_data.append(dict(zip(fields,row)))
    except:
        return dict(code = CODE_DATA_INVALID, message = 'Data Invalid')

    params = dict(data=ret_data)
    return dict(code = CODE_OK, message = 'Data Submitted',params = params)
