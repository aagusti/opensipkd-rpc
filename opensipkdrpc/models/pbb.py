import sys
from datetime import datetime
from sqlalchemy import (
    Column,
    Integer,
    Text,
    DateTime,
    ForeignKey,
    UniqueConstraint,
    String,
    SmallInteger,
    types,
    func
    )

from sqlalchemy.orm.exc import NoResultFound

from sqlalchemy.orm import (
    scoped_session,
    sessionmaker,
    relationship,
    backref
    )
import re
            
KECAMATAN = [
    ('kd_propinsi', 2, 'N'),
    ('kd_dati2', 2, 'N'),
    ('kd_kecamatan', 3, 'N'),]
    
DESA = [
    ('kd_propinsi', 2, 'N'),
    ('kd_dati2', 2, 'N'),
    ('kd_kecamatan', 3, 'N'),
    ('kd_kelurahan', 3, 'N'),]

NOP = [
    ('kd_propinsi', 2, 'N'),
    ('kd_dati2', 2, 'N'),
    ('kd_kecamatan', 3, 'N'),
    ('kd_kelurahan', 3, 'N'),
    ('kd_blok', 3, 'N'),
    ('no_urut', 4, 'N'),
    ('kd_jns_op', 1, 'N'),]
    
from ..tools import as_timezone, FixLength

from ..models import CommonModel, pbb_Base, pbb_DBSession

class Propinsi(pbb_Base, CommonModel):
    __tablename__  = 'ref_propinsi'
    __table_args__ = {'extend_existing':True, 'autoload':True}

class Dati2(pbb_Base, CommonModel):
    __tablename__  = 'ref_dati2'
    __table_args__ = {'extend_existing':True, 'autoload':True}

class Kecamatan(pbb_Base, CommonModel):
    __tablename__  = 'ref_kecamatan'
    __table_args__ = {'extend_existing':True, 'autoload':True}

class Kelurahan(pbb_Base, CommonModel):
    __tablename__  = 'ref_kelurahan'
    __table_args__ = {'extend_existing':True, 'autoload':True}

class DatObjekPajak(pbb_Base, CommonModel):
    __tablename__  = 'dat_objek_pajak'
    __table_args__ = {'extend_existing':True, 'autoload':True}

class DatSubjekPajak(pbb_Base, CommonModel):
    __tablename__  = 'dat_subjek_pajak'
    __table_args__ = {'extend_existing':True, 'autoload':True}
    
class Sppt(pbb_Base, CommonModel):
    __tablename__  = 'sppt'
    __table_args__ = {'extend_existing':True, 'autoload':True}
    
    @classmethod
    def query_data(cls):
        return pbb_DBSession.query(cls)
        
    @classmethod
    def get_by_nop(cls, p_nop):
        pkey = FixLength(NOP)
        pkey.set_raw(p_nop)
        query = cls.query_data()
        return query.filter_by(kd_propinsi = pkey['kd_propinsi'], 
                            kd_dati2 = pkey['kd_dati2'], 
                            kd_kecamatan = pkey['kd_kecamatan'], 
                            kd_kelurahan = pkey['kd_kelurahan'], 
                            kd_blok = pkey['kd_blok'], 
                            no_urut = pkey['no_urut'], 
                            kd_jns_op = pkey['kd_jns_op'],)
    @classmethod
    def get_nop_by_nop_thn(cls, p_nop, p_tahun):
        query = cls.get_by_nop(p_nop)
        return query.filter_by(thn_pajak_sppt = p_tahun)
        
    @classmethod
    def get_nop_by_kelurahan(cls, p_kode, p_tahun):
        pkey = FixLength(DESA)
        pkey.set_raw(p_kode)
        query = cls.query_data()
        return query.filter_by(kd_propinsi = pkey['kd_propinsi'], 
                            kd_dati2 = pkey['kd_dati2'], 
                            kd_kecamatan = pkey['kd_kecamatan'], 
                            kd_kelurahan = pkey['kd_kelurahan'], 
                            thn_pajak_sppt = p_tahun)
                            
    @classmethod
    def get_nop_by_kecamatan(cls, p_kode, p_tahun):
        pkey = FixLength(KECAMATAN)
        pkey.set_raw(p_kode)
        query = cls.query_data()
        return query.filter_by(kd_propinsi = pkey['kd_propinsi'], 
                            kd_dati2 = pkey['kd_dati2'], 
                            kd_kecamatan = pkey['kd_kecamatan'], 
                            kd_kelurahan = pkey['kd_kelurahan'], 
                            thn_pajak_sppt = p_tahun)
                            
    @classmethod
    def get_rekap_desa(cls, p_kode, p_tahun):
        pkey = FixLength(KECAMATAN)
        pkey.set_raw(p_kode)
        query = pbb_DBSession.query(cls.kd_propinsi, cls.kd_dati2, cls.kd_kecamatan, cls.kd_kelurahan, 
                               func.sum(cls.pbb_yg_harus_dibayar_sppt).label('tagihan')).\
                               group_by(cls.kd_propinsi, cls.kd_dati2, cls.kd_kecamatan, cls.kd_kelurahan)
        return query.filter_by(kd_propinsi = pkey['kd_propinsi'], 
                            kd_dati2 = pkey['kd_dati2'], 
                            kd_kecamatan = pkey['kd_kecamatan'], 
                            thn_pajak_sppt = p_tahun)

    @classmethod
    def get_rekap_kec(cls, p_tahun):
        query = pbb_DBSession.query(cls.kd_propinsi, cls.kd_dati2, cls.kd_kecamatan,  
                               func.sum(cls.pbb_yg_harus_dibayar_sppt).label('tagihan')).\
                               group_by(cls.kd_propinsi, cls.kd_dati2, cls.kd_kecamatan)
        return query.filter_by(thn_pajak_sppt = p_tahun)
                                             
class PembayaranSppt(pbb_Base, CommonModel):
    __tablename__  = 'pembayaran_sppt'
    __table_args__ = {'extend_existing':True, 'autoload':True}
    
    @classmethod
    def query_data(cls):
        return pbb_DBSession.query(cls)
        
    @classmethod
    def get_by_nop(cls, p_nop):
        pkey = FixLength(NOP)
        pkey.set_raw(p_nop)
        query = cls.query_data()
        return query.filter_by(kd_propinsi = pkey['kd_propinsi'], 
                            kd_dati2 = pkey['kd_dati2'], 
                            kd_kecamatan = pkey['kd_kecamatan'], 
                            kd_kelurahan = pkey['kd_kelurahan'], 
                            kd_blok = pkey['kd_blok'], 
                            no_urut = pkey['no_urut'], 
                            kd_jns_op = pkey['kd_jns_op'],)
    @classmethod
    def get_nop_by_nop_thn(cls, p_nop, p_tahun):
        query = cls.get_by_nop(p_nop)
        return query.filter_by(thn_pajak_sppt = p_tahun)
        
    @classmethod
    def get_nop_by_kelurahan(cls, p_kode, p_tahun):
        pkey = FixLength(DESA)
        pkey.set_raw(p_kode)
        query = cls.query_data()
        return query.filter_by(kd_propinsi = pkey['kd_propinsi'], 
                            kd_dati2 = pkey['kd_dati2'], 
                            kd_kecamatan = pkey['kd_kecamatan'], 
                            kd_kelurahan = pkey['kd_kelurahan'], 
                            thn_pajak_sppt = p_tahun)
                            
    @classmethod
    def get_nop_by_kecamatan(cls, p_kode, p_tahun):
        pkey = FixLength(KECAMATAN)
        pkey.set_raw(p_kode)
        query = cls.query_data()
        return query.filter_by(kd_propinsi = pkey['kd_propinsi'], 
                            kd_dati2 = pkey['kd_dati2'], 
                            kd_kecamatan = pkey['kd_kecamatan'], 
                            kd_kelurahan = pkey['kd_kelurahan'], 
                            thn_pajak_sppt = p_tahun)
    
    @classmethod
    def get_nop_by_tanggal(cls, p_kode, p_tahun):
        pkey = DateVar
        p_kode = re.sub("[^0-9]", "", p_kode)
        pkey.set_raw(p_kode)
        query = cls.query_data()
        return query.filter_by(tgl_pembayaran_sppt = pkey.get_value)
                            
    @classmethod
    def get_rekap_desa(cls, p_kode, p_tahun):
        pkey = FixLength(KECAMATAN)
        pkey.set_raw(p_kode)
        query = pbb_DBSession.query(cls.kd_propinsi, cls.kd_dati2, cls.kd_kecamatan, cls.kd_kelurahan, 
                               func.sum(cls.denda_sppt).label('denda'),
                               func.sum(cls.pbb_yg_dibayar_sppt).label('jumlah') ).\
                               group_by(cls.kd_propinsi, cls.kd_dati2, cls.kd_kecamatan, cls.kd_kelurahan)
        return query.filter_by(kd_propinsi = pkey['kd_propinsi'], 
                            kd_dati2 = pkey['kd_dati2'], 
                            kd_kecamatan = pkey['kd_kecamatan'], 
                            thn_pajak_sppt = p_tahun)

    @classmethod
    def get_rekap_kec(cls, p_tahun):
        query = pbb_DBSession.query(cls.kd_propinsi, cls.kd_dati2, cls.kd_kecamatan,  
                               func.sum(cls.denda_sppt).label('denda'),
                               func.sum(cls.pbb_yg_dibayar_sppt).label('jumlah')).\
                               group_by(cls.kd_propinsi, cls.kd_dati2, cls.kd_kecamatan)
        return query.filter_by(thn_pajak_sppt = p_tahun)
                                     