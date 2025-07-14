import sys
from datetime import datetime
from sqlalchemy import (
    and_,
    Column,
    Integer,
    Float,
    Text,
    DateTime,
    ForeignKey,
    UniqueConstraint,
    String,
    SmallInteger,
    types,
    func,
    ForeignKeyConstraint,
    )

from sqlalchemy.orm.exc import NoResultFound

from sqlalchemy.orm import (
    scoped_session,
    sessionmaker,
    relationship,
    backref
    )
import re
from ..tools import as_timezone, FixLength

from ..models import CommonModel, pbb_Base, pbb_DBSession
from .pbb_ref_wilayah import Kelurahan, Kecamatan, Dati2, KELURAHAN, KECAMATAN

            
NOP = [
    ('kd_propinsi', 2, 'N'),
    ('kd_dati2', 2, 'N'),
    ('kd_kecamatan', 3, 'N'),
    ('kd_kelurahan', 3, 'N'),
    ('kd_blok', 3, 'N'),
    ('no_urut', 4, 'N'),
    ('kd_jns_op', 1, 'N'),]
    
class DatPetaBlok(pbb_Base, CommonModel):
    __tablename__  = 'dat_peta_blok'
    __tablename__ = 'dat_peta_blok'
    kd_propinsi = Column(String(2), primary_key=True)
    kd_dati2 = Column(String(2), primary_key=True)
    kd_kecamatan = Column(String(3), primary_key=True)
    kd_kelurahan = Column(String(3), primary_key=True)
    kd_blok = Column(String(3), primary_key=True)
    status_peta_blok = Column(Float)


        
class DatOpAnggota(pbb_Base, CommonModel):
    __tablename__  = 'dat_op_anggota'
    __table_args__ = (ForeignKeyConstraint(['kd_propinsi','kd_dati2','kd_kecamatan','kd_kelurahan',
                                            'kd_blok', 'no_urut','kd_jns_op'], 
                                            ['dat_objek_pajak.kd_propinsi', 'dat_objek_pajak.kd_dati2',
                                             'dat_objek_pajak.kd_kecamatan','dat_objek_pajak.kd_kelurahan',
                                             'dat_objek_pajak.kd_blok', 'dat_objek_pajak.no_urut',
                                             'dat_objek_pajak.kd_jns_op']),
                     {'extend_existing':True})
    __tablename__ = 'dat_op_anggota'
    kd_propinsi_induk = Column(String(2), primary_key=True)
    kd_dati2_induk = Column(String(2), primary_key=True)
    kd_kecamatan_induk = Column(String(3), primary_key=True)
    kd_kelurahan_induk = Column(String(3), primary_key=True)
    kd_blok_induk = Column(String(3), primary_key=True)
    no_urut_induk = Column(String(4), primary_key=True)
    kd_jns_op_induk = Column(String(1), primary_key=True)
    kd_propinsi = Column(String(2), primary_key=True)
    kd_dati2 = Column(String(2), primary_key=True)
    kd_kecamatan = Column(String(3), primary_key=True)
    kd_kelurahan = Column(String(3), primary_key=True)
    kd_blok = Column(String(3), primary_key=True)
    no_urut = Column(String(4), primary_key=True)
    kd_jns_op = Column(String(1), primary_key=True)
    luas_bumi_beban = Column(Float)
    luas_bng_beban = Column(Float)
    nilai_sistem_bumi_beban = Column(Float)
    nilai_sistem_bng_beban = Column(Float)
    njop_bumi_beban = Column(Float)
    njop_bng_beban = Column(Float)

    
class DatObjekPajak(pbb_Base, CommonModel):
    __tablename__  = 'dat_objek_pajak'
    __tablename__ = 'dat_objek_pajak'
    kd_propinsi = Column(String(2), primary_key=True)
    kd_dati2 = Column(String(2), primary_key=True)
    kd_kecamatan = Column(String(3), primary_key=True)
    kd_kelurahan = Column(String(3), primary_key=True)
    kd_blok = Column(String(3), primary_key=True)
    no_urut = Column(String(4), primary_key=True)
    kd_jns_op = Column(String(1), primary_key=True)
    subjek_pajak_id = Column(String(30), ForeignKey('dat_subjek_pajak.subjek_pajak_id'))
    no_formulir_spop = Column(String(11))
    no_persil = Column(String(5))
    jalan_op = Column(String(30))
    blok_kav_no_op = Column(String(15))
    rw_op = Column(String(2))
    rt_op = Column(String(3))
    kd_status_cabang = Column(Float)
    kd_status_wp = Column(String(1))
    total_luas_bumi = Column(Float)
    total_luas_bng = Column(Float)
    njop_bumi = Column(Float)
    njop_bng = Column(Float)
    status_peta_op = Column(Float)
    jns_transaksi_op = Column(String(1))
    tgl_pendataan_op = Column(DateTime)
    nip_pendata = Column(String(18))
    tgl_pemeriksaan_op = Column(DateTime)
    nip_pemeriksa_op = Column(String(18))
    tgl_perekaman_op = Column(DateTime)
    nip_perekam_op = Column(String(18))
    no_sertifikat = Column(String(30))
    create_date = Column(DateTime)

    
    @classmethod
    def query_data(cls):
        return pbb_DBSession.query(cls)
        
    @classmethod
    def get_by_nop(cls, p_kode):
        pkey = FixLength(NOP)
        pkey.set_raw(p_kode)
        query = cls.query_data()
        return query.filter_by(kd_propinsi = pkey['kd_propinsi'], 
                            kd_dati2 = pkey['kd_dati2'], 
                            kd_kecamatan = pkey['kd_kecamatan'], 
                            kd_kelurahan = pkey['kd_kelurahan'], 
                            kd_blok = pkey['kd_blok'], 
                            no_urut = pkey['no_urut'], 
                            kd_jns_op = pkey['kd_jns_op'],)

    @classmethod
    def get_info_op_bphtb(cls, p_kode):
        pkey = FixLength(NOP)
        pkey.set_raw(p_kode)
        query = pbb_DBSession.query(
                  cls.jalan_op, cls.blok_kav_no_op, cls.rt_op, cls.rw_op,
                  cls.total_luas_bumi.label('luas_bumi_sppt'), cls.total_luas_bng.label('luas_bng_sppt'), 
                  cls.njop_bumi.label('njop_bumi_sppt'), cls.njop_bng.label('njop_bng_sppt'),
                  DatSubjekPajak.nm_wp,  
                  func.coalesce(DatOpAnggota.luas_bumi_beban,0).label('luas_bumi_beban'), 
                  func.coalesce(DatOpAnggota.luas_bng_beban,0).label('luas_bng_beban'), 
                  func.coalesce(DatOpAnggota.njop_bumi_beban,0).label('njop_bumi_beban'), 
                  func.coalesce(DatOpAnggota.njop_bng_beban,0).label('njop_bng_beban'), ).\
              outerjoin(DatSubjekPajak).\
              outerjoin(DatOpAnggota)
              
        return query.filter(
                            cls.kd_propinsi == pkey['kd_propinsi'], 
                            cls.kd_dati2 == pkey['kd_dati2'], 
                            cls.kd_kecamatan == pkey['kd_kecamatan'], 
                            cls.kd_kelurahan == pkey['kd_kelurahan'], 
                            cls.kd_blok == pkey['kd_blok'], 
                            cls.no_urut == pkey['no_urut'], 
                            cls.kd_jns_op == pkey['kd_jns_op'],)

                          
class DatSubjekPajak(pbb_Base, CommonModel):
    __tablename__  = 'dat_subjek_pajak'
    __tablename__ = 'dat_subjek_pajak'
    subjek_pajak_id = Column(String(30), primary_key=True)
    nm_wp = Column(String(30))
    jalan_wp = Column(String(30))
    blok_kav_no_wp = Column(String(15))
    rw_wp = Column(String(2))
    rt_wp = Column(String(3))
    kelurahan_wp = Column(String(30))
    kota_wp = Column(String(30))
    kd_pos_wp = Column(String(5))
    telp_wp = Column(String(20))
    npwp = Column(String(15))
    status_pekerjaan_wp = Column(String(1))
    email_wp = Column(String(64))
    hp_wp = Column(String(16))


class DatOpBumi(pbb_Base, CommonModel):
    __tablename__  = 'dat_op_bumi'
    __tablename__ = 'dat_op_bumi'
    kd_propinsi = Column(String(2), primary_key=True)
    kd_dati2 = Column(String(2), primary_key=True)
    kd_kecamatan = Column(String(3), primary_key=True)
    kd_kelurahan = Column(String(3), primary_key=True)
    kd_blok = Column(String(3), primary_key=True)
    no_urut = Column(String(4), primary_key=True)
    kd_jns_op = Column(String(1), primary_key=True)
    no_bumi = Column(Float, primary_key=True)
    kd_znt = Column(String(2))
    luas_bumi = Column(Float)
    jns_bumi = Column(String(1))
    nilai_sistem_bumi = Column(Float)

    @classmethod
    def query_data(cls):
        return pbb_DBSession.query(cls)

    @classmethod
    def get_by_nop(cls, p_kode):
        pkey = FixLength(NOP)
        pkey.set_raw(p_kode)
        query = cls.query_data()
        return query.filter_by(kd_propinsi=pkey['kd_propinsi'],
                               kd_dati2=pkey['kd_dati2'],
                               kd_kecamatan=pkey['kd_kecamatan'],
                               kd_kelurahan=pkey['kd_kelurahan'],
                               kd_blok=pkey['kd_blok'],
                               no_urut=pkey['no_urut'],
                               kd_jns_op=pkey['kd_jns_op'], )
    
class Sppt(pbb_Base, CommonModel):
    __tablename__  = 'sppt'
    kd_propinsi = Column(String(2), primary_key=True)
    kd_dati2 = Column(String(2), primary_key=True)
    kd_kecamatan = Column(String(3), primary_key=True)
    kd_kelurahan = Column(String(3), primary_key=True)
    kd_blok = Column(String(3), primary_key=True)
    no_urut = Column(String(4), primary_key=True)
    kd_jns_op = Column(String(1), primary_key=True)
    thn_pajak_sppt = Column(String(4), primary_key=True)
    siklus_sppt = Column(Float)
    kd_kanwil = Column(String(2))
    kd_kantor = Column(String(2))
    kd_tp = Column(String(2))
    nm_wp_sppt = Column(String(30))
    jln_wp_sppt = Column(String(30))
    blok_kav_no_wp_sppt = Column(String(15))
    rw_wp_sppt = Column(String(2))
    rt_wp_sppt = Column(String(3))
    kelurahan_wp_sppt = Column(String(30))
    kota_wp_sppt = Column(String(30))
    kd_pos_wp_sppt = Column(String(5))
    npwp_sppt = Column(String(15))
    no_persil_sppt = Column(String(5))
    kd_kls_tanah = Column(String(3))
    thn_awal_kls_tanah = Column(String(4))
    kd_kls_bng = Column(String(3))
    thn_awal_kls_bng = Column(String(4))
    tgl_jatuh_tempo_sppt = Column(DateTime)
    luas_bumi_sppt = Column(Float)
    luas_bng_sppt = Column(Float)
    njop_bumi_sppt = Column(Float)
    njop_bng_sppt = Column(Float)
    njop_sppt = Column(Float)
    njoptkp_sppt = Column(Float)
    pbb_terhutang_sppt = Column(Float)
    faktor_pengurang_sppt = Column(Float)
    pbb_yg_harus_dibayar_sppt = Column(Float)
    status_pembayaran_sppt = Column(String(1))
    status_tagihan_sppt = Column(String(1))
    status_cetak_sppt = Column(String(1))
    tgl_terbit_sppt = Column(DateTime)
    tgl_cetak_sppt = Column(DateTime)
    nip_pencetak_sppt = Column(String(18))
    create_date = Column(DateTime)
    denda_status = Column(String(1))
    denda_pbb = Column(Float)
    njkp_pct = Column(Float)
    njkp_sppt = Column(Float)
    tarif_pct = Column(Float)
    denda_pct = Column(Float)
    subjek_pajak_id = Column(String(32))

    @classmethod
    def query_data(cls):
        return pbb_DBSession.query(cls)
        
    @classmethod
    def count(cls, p_kode):
        pkey = FixLength(NOP)
        pkey.set_raw(p_kode)
        query = pbb_DBSession.query(func.count(cls.kd_propinsi))
        return query.filter_by(kd_propinsi = pkey['kd_propinsi'], 
                            kd_dati2 = pkey['kd_dati2'], 
                            kd_kecamatan = pkey['kd_kecamatan'], 
                            kd_kelurahan = pkey['kd_kelurahan'], 
                            kd_blok = pkey['kd_blok'], 
                            no_urut = pkey['no_urut'], 
                            kd_jns_op = pkey['kd_jns_op'],).scalar()
    @classmethod
    def get_by_nop(cls, p_kode):
        pkey = FixLength(NOP)
        pkey.set_raw(p_kode)
        query = cls.query_data()
        return query.filter_by(kd_propinsi = pkey['kd_propinsi'], 
                            kd_dati2 = pkey['kd_dati2'], 
                            kd_kecamatan = pkey['kd_kecamatan'], 
                            kd_kelurahan = pkey['kd_kelurahan'], 
                            kd_blok = pkey['kd_blok'], 
                            no_urut = pkey['no_urut'], 
                            kd_jns_op = pkey['kd_jns_op'],)

    @classmethod
    def get_bayar(cls, p_kode):
        pkey = FixLength(NOP)
        pkey.set_raw(p_kode)
        query = pbb_DBSession.query(
            func.concat(cls.kd_propinsi, '.').concat(cls.kd_dati2).concat('-'). \
                concat(cls.kd_kecamatan).concat('.').concat(cls.kd_kelurahan).concat('-'). \
                concat(cls.kd_blok).concat('.').concat(cls.no_urut).concat('-'). \
                concat(cls.kd_jns_op).label('nop'), cls.thn_pajak_sppt,
            cls.nm_wp_sppt, cls.jln_wp_sppt, cls.blok_kav_no_wp_sppt,
            cls.rw_wp_sppt, cls.rt_wp_sppt, cls.kelurahan_wp_sppt,
            cls.kota_wp_sppt, cls.kd_pos_wp_sppt, cls.npwp_sppt,
            cls.kd_kls_tanah, cls.kd_kls_bng,
            cls.luas_bumi_sppt, cls.luas_bng_sppt,
            cls.njop_bumi_sppt, cls.njop_bng_sppt, cls.njop_sppt,
            cls.njoptkp_sppt, cls.pbb_terhutang_sppt, cls.faktor_pengurang_sppt,
            cls.status_pembayaran_sppt,
            cls.tgl_jatuh_tempo_sppt,
            cls.pbb_yg_harus_dibayar_sppt.label('pokok'),
            func.max(PembayaranSppt.tgl_pembayaran_sppt).label('tgl_pembayaran_sppt'),
            func.sum(func.coalesce(PembayaranSppt.jml_sppt_yg_dibayar, 0)).label('bayar'),
            func.sum(func.coalesce(PembayaranSppt.denda_sppt, 0)).label('denda_sppt'), ). \
            outerjoin(PembayaranSppt, and_(
            cls.kd_propinsi == PembayaranSppt.kd_propinsi,
            cls.kd_dati2 == PembayaranSppt.kd_dati2,
            cls.kd_kecamatan == PembayaranSppt.kd_kecamatan,
            cls.kd_kelurahan == PembayaranSppt.kd_kelurahan,
            cls.kd_blok == PembayaranSppt.kd_blok,
            cls.no_urut == PembayaranSppt.no_urut,
            cls.kd_jns_op == PembayaranSppt.kd_jns_op,
            cls.thn_pajak_sppt == PembayaranSppt.thn_pajak_sppt
        )). \
            group_by(cls.kd_propinsi, cls.kd_dati2, cls.kd_kecamatan, cls.kd_kelurahan,
                     cls.kd_blok, cls.no_urut, cls.kd_jns_op, cls.thn_pajak_sppt,
                     cls.nm_wp_sppt, cls.jln_wp_sppt, cls.blok_kav_no_wp_sppt,
                     cls.rw_wp_sppt, cls.rt_wp_sppt, cls.kelurahan_wp_sppt,
                     cls.kota_wp_sppt, cls.kd_pos_wp_sppt, cls.npwp_sppt,
                     cls.kd_kls_tanah, cls.kd_kls_bng,
                     cls.luas_bumi_sppt, cls.luas_bng_sppt,
                     cls.njop_bumi_sppt, cls.njop_bng_sppt, cls.njop_sppt,
                     cls.njoptkp_sppt, cls.pbb_terhutang_sppt, cls.faktor_pengurang_sppt,
                     cls.status_pembayaran_sppt,
                     cls.tgl_jatuh_tempo_sppt,
                     cls.pbb_yg_harus_dibayar_sppt.label('pokok'), )

        return query.filter(cls.kd_propinsi == pkey['kd_propinsi'],
                            cls.kd_dati2 == pkey['kd_dati2'],
                            cls.kd_kecamatan == pkey['kd_kecamatan'],
                            cls.kd_kelurahan == pkey['kd_kelurahan'],
                            cls.kd_blok == pkey['kd_blok'],
                            cls.no_urut == pkey['no_urut'],
                            cls.kd_jns_op == pkey['kd_jns_op'], )

    @classmethod
    def get_by_nop_thn(cls, p_kode, p_tahun):
        query = cls.get_by_nop(p_kode)
        return query.filter_by(thn_pajak_sppt = p_tahun)
        
    @classmethod
    def get_info_op(cls, p_kode):
        pkey = FixLength(NOP)
        pkey.set_raw(p_kode)
        query = pbb_DBSession.query(
              func.concat(cls.kd_propinsi, '.').concat(cls.kd_dati2).concat('-').\
                   concat(cls.kd_kecamatan).concat('.').concat(cls.kd_kelurahan).concat('-').\
                   concat(cls.kd_blok).concat('.').concat(cls.no_urut).concat('-').\
                   concat(cls.kd_jns_op).label('nop'),
              cls.thn_pajak_sppt, cls.nm_wp_sppt.label('nm_wp'),
              func.concat(cls.jln_wp_sppt,', ').concat(cls.blok_kav_no_wp_sppt).label('alamat_wp'),
              func.concat(cls.rt_wp_sppt, ' / ').concat(cls.rw_wp_sppt).label('rt_rw_wp'),
              cls.kelurahan_wp_sppt.label('kelurahan_wp'), cls.kota_wp_sppt.label('kota_wp'), 
              cls.luas_bumi_sppt.label('luas_tanah'), cls.njop_bumi_sppt.label('njop_tanah'),
              cls.luas_bng_sppt.label('luas_bng'),cls.njop_bng_sppt.label('njop_bng'),
              cls.pbb_yg_harus_dibayar_sppt.label('ketetapan'), 
              cls.status_pembayaran_sppt.label('status_bayar'),
              func.concat(DatObjekPajak.jalan_op,', ').concat(DatObjekPajak.blok_kav_no_op).label('alamat_op'),
              func.concat(DatObjekPajak.rt_op,' / ').concat(DatObjekPajak.rw_op).label('rt_rw_op'),).\
              outerjoin(DatObjekPajak,  
              and_(cls.kd_propinsi == DatObjekPajak.kd_propinsi, 
                            cls.kd_dati2 == DatObjekPajak.kd_dati2, 
                            cls.kd_kecamatan == DatObjekPajak.kd_kecamatan, 
                            cls.kd_kelurahan == DatObjekPajak.kd_kelurahan, 
                            cls.kd_blok == DatObjekPajak.kd_blok, 
                            cls.no_urut == DatObjekPajak.no_urut, 
                            cls.kd_jns_op == DatObjekPajak.kd_jns_op))
        return query.filter(cls.kd_propinsi == pkey['kd_propinsi'], 
                            cls.kd_dati2 == pkey['kd_dati2'], 
                            cls.kd_kecamatan == pkey['kd_kecamatan'], 
                            cls.kd_kelurahan == pkey['kd_kelurahan'], 
                            cls.kd_blok == pkey['kd_blok'], 
                            cls.no_urut == pkey['no_urut'], 
                            cls.kd_jns_op == pkey['kd_jns_op'],)
    @classmethod
    def get_info_op_bphtb(cls, p_kode, p_tahun):
        pkey = FixLength(NOP)
        pkey.set_raw(p_kode)
        query = pbb_DBSession.query( cls.luas_bumi_sppt, cls.luas_bng_sppt, 
                  cls.njop_bumi_sppt, cls.njop_bng_sppt, DatObjekPajak.jalan_op, 
                  DatObjekPajak.blok_kav_no_op, DatObjekPajak.rt_op, DatObjekPajak.rw_op,
                  cls.nm_wp_sppt.label('nm_wp'), 
                  func.coalesce(SpptOpBersama.luas_bumi_beban_sppt,0).label('luas_bumi_beban'), 
                  func.coalesce(SpptOpBersama.luas_bng_beban_sppt,0).label('luas_bng_beban'), 
                  func.coalesce(SpptOpBersama.njop_bumi_beban_sppt,0).label('njop_bumi_beban'), 
                  func.coalesce(SpptOpBersama.njop_bng_beban_sppt,0).label('njop_bng_beban'),
                  ).\
              outerjoin(DatObjekPajak,
              and_(cls.kd_propinsi == DatObjekPajak.kd_propinsi, 
                            cls.kd_dati2 == DatObjekPajak.kd_dati2, 
                            cls.kd_kecamatan == DatObjekPajak.kd_kecamatan, 
                            cls.kd_kelurahan == DatObjekPajak.kd_kelurahan, 
                            cls.kd_blok == DatObjekPajak.kd_blok, 
                            cls.no_urut == DatObjekPajak.no_urut, 
                            cls.kd_jns_op == DatObjekPajak.kd_jns_op)).\
              outerjoin(SpptOpBersama,
              and_(cls.kd_propinsi == SpptOpBersama.kd_propinsi, 
                            cls.kd_dati2 == SpptOpBersama.kd_dati2, 
                            cls.kd_kecamatan == SpptOpBersama.kd_kecamatan, 
                            cls.kd_kelurahan == SpptOpBersama.kd_kelurahan, 
                            cls.kd_blok == SpptOpBersama.kd_blok, 
                            cls.no_urut == SpptOpBersama.no_urut, 
                            cls.kd_jns_op == SpptOpBersama.kd_jns_op,
                            cls.thn_pajak_sppt == SpptOpBersama.thn_pajak_sppt))
        return query.filter(
                            cls.kd_propinsi == pkey['kd_propinsi'], 
                            cls.kd_dati2 == pkey['kd_dati2'], 
                            cls.kd_kecamatan == pkey['kd_kecamatan'], 
                            cls.kd_kelurahan == pkey['kd_kelurahan'], 
                            cls.kd_blok == pkey['kd_blok'], 
                            cls.no_urut == pkey['no_urut'], 
                            cls.kd_jns_op == pkey['kd_jns_op'],
                            cls.thn_pajak_sppt == p_tahun)
    @classmethod
    def get_dop(cls, p_kode, p_tahun):
        pkey = FixLength(NOP)
        pkey.set_raw(p_kode)
        query = pbb_DBSession.query( func.concat(cls.kd_propinsi, '.').concat(cls.kd_dati2).concat('-').\
                   concat(cls.kd_kecamatan).concat('.').concat(cls.kd_kelurahan).concat('-').\
                   concat(cls.kd_blok).concat('.').concat(cls.no_urut).concat('-').\
                   concat(cls.kd_jns_op).label('nop'),
              cls.thn_pajak_sppt, cls.luas_bumi_sppt, cls.njop_bumi_sppt, 
              cls.luas_bng_sppt, cls.njop_bng_sppt, 
              cls.pbb_yg_harus_dibayar_sppt, cls.status_pembayaran_sppt,
              DatObjekPajak.jalan_op, DatObjekPajak.blok_kav_no_op, 
              DatObjekPajak.rt_op, DatObjekPajak.rw_op,
              func.coalesce(SpptOpBersama.luas_bumi_beban_sppt,0).label('luas_bumi_beban'), 
              func.coalesce(SpptOpBersama.luas_bng_beban_sppt,0).label('luas_bng_beban'), 
              func.coalesce(SpptOpBersama.njop_bumi_beban_sppt,0).label('njop_bumi_beban'), 
              func.coalesce(SpptOpBersama.njop_bng_beban_sppt,0).label('njop_bng_beban'),
              Kelurahan.nm_kelurahan, Kecamatan.nm_kecamatan, Dati2.nm_dati2,
              func.max(PembayaranSppt.tgl_pembayaran_sppt).label('tgl_bayar'),
              func.sum(func.coalesce(PembayaranSppt.jml_sppt_yg_dibayar,0)).label('jml_sppt_yg_dibayar'),
              func.sum(func.coalesce(PembayaranSppt.denda_sppt,0)).label('denda_sppt'),).\
              outerjoin(DatObjekPajak,
              and_(cls.kd_propinsi == DatObjekPajak.kd_propinsi, 
                            cls.kd_dati2 == DatObjekPajak.kd_dati2, 
                            cls.kd_kecamatan == DatObjekPajak.kd_kecamatan, 
                            cls.kd_kelurahan == DatObjekPajak.kd_kelurahan, 
                            cls.kd_blok == DatObjekPajak.kd_blok, 
                            cls.no_urut == DatObjekPajak.no_urut, 
                            cls.kd_jns_op == DatObjekPajak.kd_jns_op)).\
              outerjoin(SpptOpBersama,
              and_(cls.kd_propinsi == SpptOpBersama.kd_propinsi, 
                            cls.kd_dati2 == SpptOpBersama.kd_dati2, 
                            cls.kd_kecamatan == SpptOpBersama.kd_kecamatan, 
                            cls.kd_kelurahan == SpptOpBersama.kd_kelurahan, 
                            cls.kd_blok == SpptOpBersama.kd_blok, 
                            cls.no_urut == SpptOpBersama.no_urut, 
                            cls.kd_jns_op == SpptOpBersama.kd_jns_op,
                            cls.thn_pajak_sppt == SpptOpBersama.thn_pajak_sppt)).\
              outerjoin(PembayaranSppt,
              and_(cls.kd_propinsi == PembayaranSppt.kd_propinsi, 
                            cls.kd_dati2 == PembayaranSppt.kd_dati2, 
                            cls.kd_kecamatan == PembayaranSppt.kd_kecamatan, 
                            cls.kd_kelurahan == PembayaranSppt.kd_kelurahan, 
                            cls.kd_blok == PembayaranSppt.kd_blok, 
                            cls.no_urut == PembayaranSppt.no_urut, 
                            cls.kd_jns_op == PembayaranSppt.kd_jns_op,
                            cls.thn_pajak_sppt == PembayaranSppt.thn_pajak_sppt)).\
              filter(cls.kd_propinsi == Kelurahan.kd_propinsi, 
                    cls.kd_dati2 == Kelurahan.kd_dati2, 
                    cls.kd_kecamatan == Kelurahan.kd_kecamatan, 
                    cls.kd_kelurahan == Kelurahan.kd_kelurahan,).\
              filter(cls.kd_propinsi == Kecamatan.kd_propinsi, 
                    cls.kd_dati2 == Kecamatan.kd_dati2, 
                    cls.kd_kecamatan == Kecamatan.kd_kecamatan,).\
              filter(cls.kd_propinsi == Dati2.kd_propinsi, 
                    cls.kd_dati2 == Dati2.kd_dati2,).\
              group_by(cls.kd_propinsi, cls.kd_dati2, cls.kd_kecamatan, cls.kd_kelurahan, cls.kd_blok,
                    cls.no_urut, cls.kd_jns_op, cls.thn_pajak_sppt, cls.luas_bumi_sppt, cls.njop_bumi_sppt, 
                    cls.luas_bng_sppt, cls.njop_bng_sppt, cls.pbb_yg_harus_dibayar_sppt, 
                    cls.status_pembayaran_sppt, DatObjekPajak.jalan_op, DatObjekPajak.blok_kav_no_op, 
                    DatObjekPajak.rt_op, DatObjekPajak.rw_op,
                    SpptOpBersama.luas_bumi_beban_sppt, 
                    SpptOpBersama.luas_bng_beban_sppt, 
                    SpptOpBersama.njop_bumi_beban_sppt, 
                    SpptOpBersama.njop_bng_beban_sppt,
                    Kelurahan.nm_kelurahan, Kecamatan.nm_kecamatan, Dati2.nm_dati2,)
        return query.filter(
                            cls.kd_propinsi == pkey['kd_propinsi'], 
                            cls.kd_dati2 == pkey['kd_dati2'], 
                            cls.kd_kecamatan == pkey['kd_kecamatan'], 
                            cls.kd_kelurahan == pkey['kd_kelurahan'], 
                            cls.kd_blok == pkey['kd_blok'], 
                            cls.no_urut == pkey['no_urut'], 
                            cls.kd_jns_op == pkey['kd_jns_op'],
                            cls.thn_pajak_sppt==p_tahun)
    @classmethod
    def get_piutangx(cls, p_kode, p_tahun, p_count):
        pkey = FixLength(NOP)
        pkey.set_raw(p_kode)
        p_tahun_awal = str(int(p_tahun)-p_count+1)
        query = pbb_DBSession.query(func.sum(cls.pbb_yg_harus_dibayar_sppt).label('pokok'), 
                                    func.sum(PembayaranSppt.denda_sppt).label('denda_sppt'),
                                    func.sum(PembayaranSppt.jml_sppt_yg_dibayar).label('bayar'),
                                    func.sum(cls.pbb_yg_harus_dibayar_sppt-
                                             (PembayaranSppt.jml_sppt_yg_dibayar-
                                              PembayaranSppt.denda_sppt)).label('sisa')
                                    ).\
              outerjoin(PembayaranSppt).\
              filter(cls.kd_propinsi == pkey['kd_propinsi'], 
                     cls.kd_dati2 == pkey['kd_dati2'], 
                     cls.kd_kecamatan == pkey['kd_kecamatan'], 
                     cls.kd_kelurahan == pkey['kd_kelurahan'], 
                     cls.kd_blok == pkey['kd_blok'], 
                     cls.no_urut == pkey['no_urut'], 
                     cls.kd_jns_op == pkey['kd_jns_op']).\
              filter(cls.thn_pajak_sppt.between(p_tahun_awal,p_tahun))
        return query

    @classmethod
    def get_piutang(cls, p_kode, p_tahun, p_count):
        pkey = FixLength(NOP)
        pkey.set_raw(p_kode)
        p_tahun_awal = str(int(p_tahun)-p_count+1)
        q1 = pbb_DBSession.query(cls.thn_pajak_sppt,(cls.pbb_yg_harus_dibayar_sppt).label('pokok'),
                                   cls.tgl_jatuh_tempo_sppt, cls.nm_wp_sppt,
                                   func.sum(PembayaranSppt.denda_sppt).label('denda_sppt'),
                                   func.sum(PembayaranSppt.jml_sppt_yg_dibayar).label('bayar'),
                                   func.sum(cls.pbb_yg_harus_dibayar_sppt - func.sum(
                                            (func.coalesce(PembayaranSppt.jml_sppt_yg_dibayar,0)-
                                             func.coalesce(PembayaranSppt.denda_sppt,0)))).label('sisa')

                                    ).\
              outerjoin(PembayaranSppt, and_(
                  cls.kd_propinsi==PembayaranSppt.kd_propinsi,
                  cls.kd_dati2==PembayaranSppt.kd_dati2,
                  cls.kd_kecamatan==PembayaranSppt.kd_kecamatan,
                  cls.kd_kelurahan==PembayaranSppt.kd_kelurahan,
                  cls.kd_blok==PembayaranSppt.kd_blok,
                  cls.no_urut==PembayaranSppt.no_urut,
                  cls.kd_jns_op==PembayaranSppt.kd_jns_op,
                  cls.thn_pajak_sppt==PembayaranSppt.thn_pajak_sppt
                  )).\
              filter(
                     cls.kd_propinsi == pkey['kd_propinsi'],
                     cls.kd_dati2 == pkey['kd_dati2'],
                     cls.kd_kecamatan == pkey['kd_kecamatan'],
                     cls.kd_kelurahan == pkey['kd_kelurahan'],
                     cls.kd_blok == pkey['kd_blok'],
                     cls.no_urut == pkey['no_urut'],
                     cls.kd_jns_op == pkey['kd_jns_op']).\
              filter(cls.thn_pajak_sppt.between(p_tahun_awal,p_tahun)).\
              filter(cls.status_pembayaran_sppt < '2', cls.pbb_yg_harus_dibayar_sppt > 0).\
              group_by(cls.thn_pajak_sppt, cls.pbb_yg_harus_dibayar_sppt,cls.tgl_jatuh_tempo_sppt, cls.nm_wp_sppt).subquery()

        query = pbb_DBSession.query(func.sum(q1.c.pokok).label('pokok'),
                                    func.sum(q1.c.denda_sppt).label('denda_sppt'),
                                    func.sum(q1.c.bayar).label('bayar'),
                                    func.sum(q1.c.sisa).label('sisa'),
                                    )

        return query
        
    @classmethod
    def get_by_kelurahan_thn(cls, p_kode, p_tahun):
        pkey = FixLength(DESA)
        pkey.set_raw(p_kode)
        query = cls.query_data()
        return query.filter_by(kd_propinsi = pkey['kd_propinsi'], 
                            kd_dati2 = pkey['kd_dati2'], 
                            kd_kecamatan = pkey['kd_kecamatan'], 
                            kd_kelurahan = pkey['kd_kelurahan'], 
                            thn_pajak_sppt = p_tahun)
                            
    @classmethod
    def get_by_kecamatan_thn(cls, p_kode, p_tahun):
        pkey = FixLength(KECAMATAN)
        pkey.set_raw(p_kode)
        query = cls.query_data()
        return query.filter_by(kd_propinsi = pkey['kd_propinsi'], 
                            kd_dati2 = pkey['kd_dati2'], 
                            kd_kecamatan = pkey['kd_kecamatan'], 
                            kd_kelurahan = pkey['kd_kelurahan'], 
                            thn_pajak_sppt = p_tahun)
                            
    @classmethod
    def get_rekap_by_kecamatan_thn(cls, p_kode, p_tahun):
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
    def get_rekap_by_tahun(cls, p_tahun):
        query = pbb_DBSession.query(cls.kd_propinsi, cls.kd_dati2, cls.kd_kecamatan,  
                               func.sum(cls.pbb_yg_harus_dibayar_sppt).label('tagihan')).\
                               group_by(cls.kd_propinsi, cls.kd_dati2, cls.kd_kecamatan)
        return query.filter_by(thn_pajak_sppt = p_tahun)

    @classmethod
    def get_transaksi(cls, p_kode):
        pkey = FixLength(NOP)
        pkey.set_raw(p_kode)
        q = pbb_DBSession.query(func.concat(cls.kd_propinsi, '.').concat(cls.kd_dati2).concat('-').\
                   concat(cls.kd_kecamatan).concat('.').concat(cls.kd_kelurahan).concat('-').\
                   concat(cls.kd_blok).concat('.').concat(cls.no_urut).concat('-').\
                   concat(cls.kd_jns_op).label('nop'),
                func.concat(DatObjekPajak.jalan_op, ', ').concat(DatObjekPajak.blok_kav_no_op).label('alamat_op'),
                func.concat(DatObjekPajak.rt_op, ' / ').concat(DatObjekPajak.rw_op).label('rt_rw_op'),
                Kelurahan.nm_kelurahan.label('kelurahan_op'),
                Kecamatan.nm_kecamatan.label('kecamatan_op'),

                cls.nm_wp_sppt,
                func.concat(cls.jln_wp_sppt, ', ').concat(cls.blok_kav_no_wp_sppt).label('alamat_wp'),
                func.concat(cls.rt_wp_sppt, ' / ').concat(cls.rw_wp_sppt).label('rt_rw_wp'),
                cls.kelurahan_wp_sppt.label('kelurahan_wp'),
                cls.kota_wp_sppt.label('kota_wp'),

                cls.thn_pajak_sppt,
                cls.luas_bumi_sppt, #
                cls.njop_bumi_sppt, #
                cls.luas_bng_sppt,  #
                cls.njop_bng_sppt,  #
                cls.njop_sppt,
                cls.pbb_yg_harus_dibayar_sppt, #
                func.to_char(func.max(cls.tgl_jatuh_tempo_sppt),'dd-mm-yyyy').label('tgl_jatuh_tempo_sppt'),
                cls.status_pembayaran_sppt, #

                func.coalesce(func.sum(PembayaranSppt.jml_sppt_yg_dibayar),0).label('jml_sppt_yg_dibayar'), #
                # case([cast(cls.status_pembayaran_sppt, Integer) == 0,
                #         func.coalesce(hit_denda(cast(cls.pbb_yg_harus_dibayar_sppt, BigInteger),2,date(cls.tgl_jatuh_tempo_sppt)),0),],
                #     else_ = func.sum(coalesce(PembayaranSppt.denda_sppt,0))).label('jml_denda'),
                func.sum(func.coalesce(PembayaranSppt.denda_sppt, 0)).label('denda_sppt'), #
                func.to_char(func.max(PembayaranSppt.tgl_pembayaran_sppt),'dd-mm-yyyy').label('tgl_pembayaran_sppt'), #
                )

        q = q.outerjoin(DatObjekPajak, and_(
                cls.kd_propinsi==DatObjekPajak.kd_propinsi,
                cls.kd_dati2==DatObjekPajak.kd_dati2,
                cls.kd_kecamatan==DatObjekPajak.kd_kecamatan,
                cls.kd_kelurahan==DatObjekPajak.kd_kelurahan,
                cls.kd_blok==DatObjekPajak.kd_blok,
                cls.no_urut==DatObjekPajak.no_urut,
                cls.kd_jns_op==DatObjekPajak.kd_jns_op,
                ))

        q = q.outerjoin(PembayaranSppt, and_(
                cls.kd_propinsi==PembayaranSppt.kd_propinsi,
                cls.kd_dati2==PembayaranSppt.kd_dati2,
                cls.kd_kecamatan==PembayaranSppt.kd_kecamatan,
                cls.kd_kelurahan==PembayaranSppt.kd_kelurahan,
                cls.kd_blok==PembayaranSppt.kd_blok,
                cls.no_urut==PembayaranSppt.no_urut,
                cls.kd_jns_op==PembayaranSppt.kd_jns_op,
                cls.thn_pajak_sppt==PembayaranSppt.thn_pajak_sppt
                ))

        q = q.outerjoin(Kecamatan, and_(
                cls.kd_propinsi==Kecamatan.kd_propinsi,
                cls.kd_dati2==Kecamatan.kd_dati2,
                cls.kd_kecamatan==Kecamatan.kd_kecamatan,
                ))

        q = q.outerjoin(Kelurahan, and_(
                cls.kd_propinsi==Kelurahan.kd_propinsi,
                cls.kd_dati2==Kelurahan.kd_dati2,
                cls.kd_kecamatan==Kelurahan.kd_kecamatan,
                cls.kd_kelurahan==Kelurahan.kd_kelurahan,
                ))

        q = q.filter(cls.kd_propinsi == pkey['kd_propinsi'],
                cls.kd_dati2 == pkey['kd_dati2'],
                cls.kd_kecamatan == pkey['kd_kecamatan'],
                cls.kd_kelurahan == pkey['kd_kelurahan'],
                cls.kd_blok == pkey['kd_blok'],
                cls.no_urut == pkey['no_urut'],
                cls.kd_jns_op == pkey['kd_jns_op'],
                cls.status_pembayaran_sppt != '2')

        q = q.group_by(cls.kd_propinsi, cls.kd_dati2, cls.kd_kecamatan, cls.kd_kelurahan, cls.kd_blok, cls.no_urut, cls.kd_jns_op,
                DatObjekPajak.jalan_op, DatObjekPajak.blok_kav_no_op, DatObjekPajak.rt_op, DatObjekPajak.rw_op,
                DatObjekPajak.total_luas_bumi, DatObjekPajak.total_luas_bng, cls.nm_wp_sppt,
                cls.jln_wp_sppt, cls.blok_kav_no_wp_sppt, cls.rt_wp_sppt, cls.rw_wp_sppt, cls.kelurahan_wp_sppt, cls.kota_wp_sppt,
                cls.thn_pajak_sppt, cls.luas_bumi_sppt, cls.njop_bumi_sppt, cls.luas_bng_sppt, cls.njop_bng_sppt, cls.njop_sppt,
                cls.pbb_yg_harus_dibayar_sppt, cls.status_pembayaran_sppt,
                Kelurahan.nm_kelurahan, Kecamatan.nm_kecamatan
                )

        q = q.order_by(cls.thn_pajak_sppt)
        return q


class SpptOpBersama(pbb_Base, CommonModel):
    __tablename__  = 'sppt_op_bersama'
    __table_args__ = (ForeignKeyConstraint(['kd_propinsi','kd_dati2','kd_kecamatan','kd_kelurahan',
                                            'kd_blok', 'no_urut','kd_jns_op', 'thn_pajak_sppt'], 
                                            ['sppt.kd_propinsi', 'sppt.kd_dati2',
                                             'sppt.kd_kecamatan','sppt.kd_kelurahan',
                                             'sppt.kd_blok', 'sppt.no_urut',
                                             'sppt.kd_jns_op','sppt.thn_pajak_sppt' ]),
                     {'extend_existing':True})
    __tablename__ = 'sppt_op_bersama'
    kd_propinsi = Column(String(2), primary_key=True)
    kd_dati2 = Column(String(2), primary_key=True)
    kd_kecamatan = Column(String(3), primary_key=True)
    kd_kelurahan = Column(String(3), primary_key=True)
    kd_blok = Column(String(3), primary_key=True)
    no_urut = Column(String(4), primary_key=True)
    kd_jns_op = Column(String(1), primary_key=True)
    thn_pajak_sppt = Column(String(4), primary_key=True)
    kd_kls_tanah = Column(String(3))
    thn_awal_kls_tanah = Column(String(4))
    kd_kls_bng = Column(String(3))
    thn_awal_kls_bng = Column(String(4))
    luas_bumi_beban_sppt = Column(Float)
    luas_bng_beban_sppt = Column(Float)
    njop_bumi_beban_sppt = Column(Float)
    njop_bng_beban_sppt = Column(Float)

        
class PembayaranSppt(pbb_Base, CommonModel):
    __tablename__  = 'pembayaran_sppt'
    __table_args__ = (ForeignKeyConstraint(['kd_propinsi','kd_dati2','kd_kecamatan','kd_kelurahan',
                                            'kd_blok', 'no_urut','kd_jns_op', 'thn_pajak_sppt'], 
                                            ['sppt.kd_propinsi', 'sppt.kd_dati2',
                                             'sppt.kd_kecamatan','sppt.kd_kelurahan',
                                             'sppt.kd_blok', 'sppt.no_urut',
                                             'sppt.kd_jns_op','sppt.thn_pajak_sppt' ]),
                     {'extend_existing':True})
    __tablename__ = 'pembayaran_sppt'
    kd_propinsi = Column(String(2), primary_key=True)
    kd_dati2 = Column(String(2), primary_key=True)
    kd_kecamatan = Column(String(3), primary_key=True)
    kd_kelurahan = Column(String(3), primary_key=True)
    kd_blok = Column(String(3), primary_key=True)
    no_urut = Column(String(4), primary_key=True)
    kd_jns_op = Column(String(1), primary_key=True)
    thn_pajak_sppt = Column(String(4), primary_key=True)
    pembayaran_sppt_ke = Column(Float, primary_key=True)
    kd_kanwil = Column(String(2), primary_key=True)
    kd_kantor = Column(String(2), primary_key=True)
    kd_tp = Column(String(2), primary_key=True)
    denda_sppt = Column(Float)
    jml_sppt_yg_dibayar = Column(Float)
    tgl_pembayaran_sppt = Column(DateTime)
    tgl_rekam_byr_sppt = Column(DateTime)
    nip_rekam_byr_sppt = Column(String(18))
    tgl_bayar = Column(DateTime)
    discount = Column(Float)

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
    def get_by_nop_thn(cls, p_nop, p_tahun):
        query = cls.get_by_nop(p_nop)
        return query.filter_by(thn_pajak_sppt = p_tahun)
        
    @classmethod
    def get_by_kelurahan(cls, p_kode, p_tahun):
        pkey = FixLength(DESA)
        pkey.set_raw(p_kode)
        query = cls.query_data()
        return query.filter_by(kd_propinsi = pkey['kd_propinsi'], 
                            kd_dati2 = pkey['kd_dati2'], 
                            kd_kecamatan = pkey['kd_kecamatan'], 
                            kd_kelurahan = pkey['kd_kelurahan'], 
                            thn_pajak_sppt = p_tahun)
                            
    @classmethod
    def get_by_kecamatan(cls, p_kode, p_tahun):
        pkey = FixLength(KECAMATAN)
        pkey.set_raw(p_kode)
        query = cls.query_data()
        return query.filter_by(kd_propinsi = pkey['kd_propinsi'], 
                            kd_dati2 = pkey['kd_dati2'], 
                            kd_kecamatan = pkey['kd_kecamatan'], 
                            kd_kelurahan = pkey['kd_kelurahan'], 
                            thn_pajak_sppt = p_tahun)
    
    @classmethod
    def get_by_tanggal(cls, p_kode, p_tahun):
        pkey = DateVar
        p_kode = re.sub("[^0-9]", "", p_kode)
        pkey.set_raw(p_kode)
        query = cls.query_data()
        return query.filter_by(tgl_pembayaran_sppt = pkey.get_value)
                            
    @classmethod
    def get_rekap_by_kecamatan(cls, p_kode, p_tahun):
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
    def get_rekap_by_thn(cls, p_tahun):
        query = pbb_DBSession.query(cls.kd_propinsi, cls.kd_dati2, cls.kd_kecamatan,  
                               func.sum(cls.denda_sppt).label('denda'),
                               func.sum(cls.pbb_yg_dibayar_sppt).label('jumlah')).\
                               group_by(cls.kd_propinsi, cls.kd_dati2, cls.kd_kecamatan)
        return query.filter_by(thn_pajak_sppt = p_tahun)
                                     
