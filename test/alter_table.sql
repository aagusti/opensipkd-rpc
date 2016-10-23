CREATE UNIQUE INDEX sppt_ukey on pbb.sppt 
 (kd_propinsi , kd_dati2 , kd_kecamatan , kd_kelurahan , kd_blok , no_urut , kd_jns_op , thn_pajak_sppt );
CREATE UNIQUE INDEX psppt_ukey on pbb.pembayaran_sppt 
(kd_propinsi , kd_dati2 , kd_kecamatan , kd_kelurahan , kd_blok , no_urut , kd_jns_op , thn_pajak_sppt , 
pembayaran_sppt_ke , kd_kanwil , kd_kantor , kd_tp ) ;

ALTER TABLE pbb.pembayaran_sppt add CONSTRAINT psppt_sppt_fk 
            FOREIGN KEY (kd_propinsi, kd_dati2, kd_kecamatan, kd_kelurahan, kd_blok, no_urut, kd_jns_op, thn_pajak_sppt)
            REFERENCES pbb.sppt (kd_propinsi, kd_dati2, kd_kecamatan, kd_kelurahan, kd_blok, no_urut, kd_jns_op, thn_pajak_sppt) MATCH SIMPLE
            ON UPDATE CASCADE ON DELETE CASCADE;
      
delete from pbb.pembayaran_sppt 
where (kd_propinsi,kd_dati2, kd_kecamatan, kd_kelurahan, kd_blok, no_urut, kd_jns_op,
       thn_pajak_sppt)
       in (
select p.kd_propinsi,p.kd_dati2, p.kd_kecamatan, p.kd_kelurahan, p.kd_blok, p.no_urut, p.kd_jns_op,
       p.thn_pajak_sppt
from pbb.pembayaran_sppt p 
left join pbb.sppt s on(p.kd_propinsi = s.kd_propinsi
and p.kd_dati2 = s.kd_dati2 
and p.kd_kecamatan = s.kd_kecamatan 
and p.kd_kelurahan = s.kd_kelurahan 
and p.kd_blok = s.kd_blok 
and p.no_urut = s.no_urut 
and p.kd_jns_op = s.kd_jns_op
and p.thn_pajak_sppt = s.thn_pajak_sppt)
where s.thn_pajak_sppt is null
);
