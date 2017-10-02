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
    func,
    ForeignKeyConstraint,
    literal_column,
    and_
    )
from sqlalchemy.orm import aliased

from sqlalchemy.orm.exc import NoResultFound

from sqlalchemy.orm import (
    scoped_session,
    sessionmaker,
    relationship,
    backref,
    #primary_join
    )
import re
from ..tools import as_timezone, FixLength

from ..models import CommonModel, pbb_Base, pbb_DBSession
from pbb_ref_wilayah import Kelurahan, Kecamatan, Dati2, KELURAHAN, KECAMATAN


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
    __table_args__ = {'extend_existing':True, 'autoload':True,
                      'schema': pbb_Base.pbb_schema}
    """dsp = relationship("DatSubjekPajak",
                  primaryjoin="DatObjekPajak.subjek_pajak_id == DatSubjekPajaksubjek_pajak_id")
    """

class DatOpAnggota(pbb_Base, CommonModel):
    __tablename__  = 'dat_op_anggota'
    __table_args__ = (ForeignKeyConstraint(['kd_propinsi','kd_dati2','kd_kecamatan','kd_kelurahan',
                                            'kd_blok', 'no_urut','kd_jns_op'],
                                            ['dat_objek_pajak.kd_propinsi', 'dat_objek_pajak.kd_dati2',
                                             'dat_objek_pajak.kd_kecamatan','dat_objek_pajak.kd_kelurahan',
                                             'dat_objek_pajak.kd_blok', 'dat_objek_pajak.no_urut',
                                             'dat_objek_pajak.kd_jns_op']),
                     {'extend_existing':True, 'autoload':True,
                      'schema': pbb_Base.pbb_schema})
    """
    dop = relationship("DatObjekPajak",

                  primaryjoin="and_(DatObjekPajak.kd_propinsi == DatOpAnggota.kd_propinsi, \
                     DatObjekPajak.kd_dati2 == DatOpAnggota.kd_dati2, \
                     DatObjekPajak.kd_kecamatan == DatOpAnggota.kd_kecamatan, \
                     DatObjekPajak.kd_kelurahan == DatOpAnggota.kd_kelurahan, \
                     DatObjekPajak.kd_blok == DatOpAnggota.kd_blok, \
                     DatObjekPajak.no_urut == DatOpAnggota.no_urut, \
                     DatObjekPajak.kd_jns_op == DatOpAnggota.kd_jns_op,)
                     "
    )
    """


class DatObjekPajak(pbb_Base, CommonModel):
    __tablename__  = 'dat_objek_pajak'
    __table_args__ = {'extend_existing':True, 'autoload':True,
                      'schema': pbb_Base.pbb_schema}
    """dsp = relationship("DatSubjekPajak",
                  primaryjoin="DatObjekPajak.subjek_pajak_id == DatSubjekPajaksubjek_pajak_id")
    """

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
    __table_args__ = {'extend_existing':True, 'autoload':True,
                      'schema': pbb_Base.pbb_schema}

class DatOpBumi(pbb_Base, CommonModel):
    __tablename__  = 'dat_op_bumi'
    __table_args__ = {'extend_existing':True, 'autoload':True,
                      'schema': pbb_Base.pbb_schema}

class Sppt(pbb_Base, CommonModel):
    __tablename__  = 'sppt'
    __table_args__ = {'extend_existing':True, 'autoload':True,
                      'schema': pbb_Base.pbb_schema}

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
              func.concat(cls.kd_propinsi, '.').concat(cls.kd_dati2).concat('-').\
                   concat(cls.kd_kecamatan).concat('.').concat(cls.kd_kelurahan).concat('-').\
                   concat(cls.kd_blok).concat('.').concat(cls.no_urut).concat('-').\
                   concat(cls.kd_jns_op).label('nop'), cls.thn_pajak_sppt,
			cls.nm_wp_sppt,	cls.jln_wp_sppt, cls.blok_kav_no_wp_sppt,
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
            func.sum(func.coalesce(PembayaranSppt.jml_sppt_yg_dibayar,0)).label('bayar'),
            func.sum(func.coalesce(PembayaranSppt.denda_sppt,0)).label('denda_sppt'),).\
			outerjoin(PembayaranSppt,and_(
                            cls.kd_propinsi==PembayaranSppt.kd_propinsi,
                            cls.kd_dati2==PembayaranSppt.kd_dati2,
                            cls.kd_kecamatan==PembayaranSppt.kd_kecamatan,
                            cls.kd_kelurahan==PembayaranSppt.kd_kelurahan,
                            cls.kd_blok==PembayaranSppt.kd_blok,
                            cls.no_urut==PembayaranSppt.no_urut,
                            cls.kd_jns_op==PembayaranSppt.kd_jns_op,
                            cls.thn_pajak_sppt==PembayaranSppt.thn_pajak_sppt
                            )).\
            group_by(cls.kd_propinsi, cls.kd_dati2, cls.kd_kecamatan, cls.kd_kelurahan,
                    cls.kd_blok, cls.no_urut, cls.kd_jns_op, cls.thn_pajak_sppt,
                    cls.nm_wp_sppt,	cls.jln_wp_sppt, cls.blok_kav_no_wp_sppt,
                    cls.rw_wp_sppt, cls.rt_wp_sppt, cls.kelurahan_wp_sppt,
                    cls.kota_wp_sppt, cls.kd_pos_wp_sppt, cls.npwp_sppt,
                    cls.kd_kls_tanah, cls.kd_kls_bng,
                    cls.luas_bumi_sppt, cls.luas_bng_sppt,
                    cls.njop_bumi_sppt, cls.njop_bng_sppt, cls.njop_sppt,
                    cls.njoptkp_sppt, cls.pbb_terhutang_sppt, cls.faktor_pengurang_sppt,
                    cls.status_pembayaran_sppt,
                    cls.tgl_jatuh_tempo_sppt,
                    cls.pbb_yg_harus_dibayar_sppt.label('pokok'),)

        return query.filter(cls.kd_propinsi == pkey['kd_propinsi'],
                            cls.kd_dati2 == pkey['kd_dati2'],
                            cls.kd_kecamatan == pkey['kd_kecamatan'],
                            cls.kd_kelurahan == pkey['kd_kelurahan'],
                            cls.kd_blok == pkey['kd_blok'],
                            cls.no_urut == pkey['no_urut'],
                            cls.kd_jns_op == pkey['kd_jns_op'],)



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
              filter(cls.kd_propinsi == DatObjekPajak.kd_propinsi,
                     cls.kd_dati2 == DatObjekPajak.kd_dati2,
                     cls.kd_kecamatan == DatObjekPajak.kd_kecamatan,
                     cls.kd_kelurahan == DatObjekPajak.kd_kelurahan,
                     cls.kd_blok == DatObjekPajak.kd_blok,
                     cls.no_urut == DatObjekPajak.no_urut,
                     cls.kd_jns_op == DatObjekPajak.kd_jns_op)
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
        q = pbb_DBSession.query(cls.luas_bumi_sppt, cls.luas_bng_sppt,
                cls.njop_bumi_sppt, cls.njop_bng_sppt, DatObjekPajak.jalan_op,
                DatObjekPajak.blok_kav_no_op, DatObjekPajak.rt_op, DatObjekPajak.rw_op,
                cls.nm_wp_sppt.label('nm_wp'),
                func.coalesce(SpptOpBersama.luas_bumi_beban_sppt,0).label('luas_bumi_beban'),
                func.coalesce(SpptOpBersama.luas_bng_beban_sppt,0).label('luas_bng_beban'),
                func.coalesce(SpptOpBersama.njop_bumi_beban_sppt,0).label('njop_bumi_beban'),
                func.coalesce(SpptOpBersama.njop_bng_beban_sppt,0).label('njop_bng_beban'))
        q = q.filter(
                cls.kd_propinsi == DatObjekPajak.kd_propinsi,
                cls.kd_dati2 == DatObjekPajak.kd_dati2,
                cls.kd_kecamatan == DatObjekPajak.kd_kecamatan,
                cls.kd_kelurahan == DatObjekPajak.kd_kelurahan,
                cls.kd_blok == DatObjekPajak.kd_blok,
                cls.no_urut == DatObjekPajak.no_urut,
                cls.kd_jns_op == DatObjekPajak.kd_jns_op)
        q = q.outerjoin(SpptOpBersama, and_(
                cls.kd_propinsi==SpptOpBersama.kd_propinsi,
                cls.kd_dati2==SpptOpBersama.kd_dati2,
                cls.kd_kecamatan==SpptOpBersama.kd_kecamatan,
                cls.kd_kelurahan==SpptOpBersama.kd_kelurahan,
                cls.kd_blok==SpptOpBersama.kd_blok,
                cls.no_urut==SpptOpBersama.no_urut,
                cls.kd_jns_op==SpptOpBersama.kd_jns_op,
                ))
        return q.filter(cls.kd_propinsi == pkey['kd_propinsi'],
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
              cls.luas_bng_sppt, cls.njop_bng_sppt, cls.nm_wp_sppt,
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
              outerjoin(DatObjekPajak, and_(
                            cls.kd_propinsi==DatObjekPajak.kd_propinsi,
                            cls.kd_dati2==DatObjekPajak.kd_dati2,
                            cls.kd_kecamatan==DatObjekPajak.kd_kecamatan,
                            cls.kd_kelurahan==DatObjekPajak.kd_kelurahan,
                            cls.kd_blok==DatObjekPajak.kd_blok,
                            cls.no_urut==DatObjekPajak.no_urut,
                            cls.kd_jns_op==DatObjekPajak.kd_jns_op,
                            )).\
              outerjoin(SpptOpBersama, and_(
                  cls.kd_propinsi==SpptOpBersama.kd_propinsi,
                  cls.kd_dati2==SpptOpBersama.kd_dati2,
                  cls.kd_kecamatan==SpptOpBersama.kd_kecamatan,
                  cls.kd_kelurahan==SpptOpBersama.kd_kelurahan,
                  cls.kd_blok==SpptOpBersama.kd_blok,
                  cls.no_urut==SpptOpBersama.no_urut,
                  cls.kd_jns_op==SpptOpBersama.kd_jns_op,
                  cls.thn_pajak_sppt==SpptOpBersama.thn_pajak_sppt)).\
              outerjoin(PembayaranSppt,and_(
                  cls.kd_propinsi==PembayaranSppt.kd_propinsi,
                  cls.kd_dati2==PembayaranSppt.kd_dati2,
                  cls.kd_kecamatan==PembayaranSppt.kd_kecamatan,
                  cls.kd_kelurahan==PembayaranSppt.kd_kelurahan,
                  cls.kd_blok==PembayaranSppt.kd_blok,
                  cls.no_urut==PembayaranSppt.no_urut,
                  cls.kd_jns_op==PembayaranSppt.kd_jns_op,
                  cls.thn_pajak_sppt==PembayaranSppt.thn_pajak_sppt
                  )).\
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
                    cls.luas_bng_sppt, cls.njop_bng_sppt, cls.nm_wp_sppt, cls.pbb_yg_harus_dibayar_sppt,
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
    def get_piutang(cls, p_kode, p_tahun, p_count):
        #Digunakan untuk menampilkan sppt dan pembayarannya
        pkey = FixLength(NOP)
        pkey.set_raw(p_kode)
        p_tahun_awal = str(int(p_tahun)-p_count+1)

        q1 = pbb_DBSession.query(cls.thn_pajak_sppt,(cls.pbb_yg_harus_dibayar_sppt).label('pokok'),
                                   cls.tgl_jatuh_tempo_sppt, cls.nm_wp_sppt,
                                   func.sum(PembayaranSppt.denda_sppt).label('denda_sppt'),
                                   func.sum(PembayaranSppt.jml_sppt_yg_dibayar).label('bayar'),
                                   (cls.pbb_yg_harus_dibayar_sppt - func.sum(
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
              filter(cls.thn_pajak_sppt.between(p_tahun_awal,p_tahun)
                    ).\
              group_by(cls.thn_pajak_sppt, cls.pbb_yg_harus_dibayar_sppt,
                      cls.tgl_jatuh_tempo_sppt, cls.nm_wp_sppt).subquery()

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

class SpptOpBersama(pbb_Base, CommonModel):
    __tablename__  = 'sppt_op_bersama'
    __table_args__ = (
        #ForeignKeyConstraint(['kd_propinsi','kd_dati2','kd_kecamatan','kd_kelurahan',
        #                      'kd_blok', 'no_urut','kd_jns_op', 'thn_pajak_sppt'],
        #                     ['sppt.kd_propinsi', 'sppt.kd_dati2',
        #                      'sppt.kd_kecamatan','sppt.kd_kelurahan',
        #                      'sppt.kd_blok', 'sppt.no_urut',
        #                      'sppt.kd_jns_op','sppt.thn_pajak_sppt']),
        {'extend_existing':True, 'autoload':True,
         'schema': pbb_Base.pbb_schema})
    #parent = relationship('Sppt', primaryjoin='and_('\
    #            'foreign(SpptOpBersama.kd_propinsi) == Sppt.kd_propinsi,'\
    #            'foreign(SpptOpBersama.kd_dati2) == Sppt.kd_dati2,'\
    #            'foreign(SpptOpBersama.kd_kecamatan) == Sppt.kd_kecamatan,'\
    #            'foreign(SpptOpBersama.kd_kelurahan) == Sppt.kd_kelurahan,'\
    #            'foreign(SpptOpBersama.kd_blok) == Sppt.kd_blok,'\
    #            'foreign(SpptOpBersama.no_urut) == Sppt.no_urut,'\
    #            'foreign(SpptOpBersama.kd_jns_op) == Sppt.kd_jns_op)')

class PembayaranSppt(pbb_Base, CommonModel):
    __tablename__  = 'pembayaran_sppt'
    __table_args__ = (
        ForeignKeyConstraint(['kd_propinsi','kd_dati2','kd_kecamatan','kd_kelurahan',
                              'kd_blok', 'no_urut','kd_jns_op', 'thn_pajak_sppt'],
                              ['sppt.kd_propinsi', 'sppt.kd_dati2',
                               'sppt.kd_kecamatan','sppt.kd_kelurahan',
                               'sppt.kd_blok', 'sppt.no_urut',
                               'sppt.kd_jns_op','sppt.thn_pajak_sppt']),
        {'extend_existing':True, 'autoload':True,
         'schema': pbb_Base.pbb_schema})
    #sppt = relationship("Sppt",
    #                      backref=backref('pembayaransppt'),
    #                      primaryjoin='foreign(Sppt.no_urut) == remote(PembyaranSppt.no_urut)')
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

