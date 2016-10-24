--QUERY SCORE KECAMATAN
SELECT drv2.kd_propinsi , drv2.kd_dati2 , drv2.kd_kecamatan, k2.nm_kecamatan, 
       c_sppt, c_rp_sppt, c_stts, c_rp_stts, 
       l_sppt, l_rp_sppt, l_stts, l_rp_stts, 
       case when c_rp_sppt = 0 then 1
            else c_rp_stts/c_rp_sppt end *25 +
       case when c_sppt = 0 then 1
            else c_stts/c_sppt*30 end +  
       case when l_rp_sppt = 0 then 1
            else l_rp_stts/l_rp_sppt end *20 +
       case when l_sppt = 0 then 1 
            else l_stts/l_sppt end *25 score
FROM( 

SELECT kd_propinsi , kd_dati2 , kd_kecamatan,
       SUM(c_sppt) c_sppt, sum(c_rp_sppt) c_rp_sppt, SUM(c_stts) c_stts, SUM(c_rp_stts) c_rp_stts, 
       SUM(l_sppt) l_sppt, SUM(l_rp_sppt) l_rp_sppt, SUM(l_stts) l_stts, SUM(l_rp_stts) l_rp_stts 
FROM(
/**/
--KETETAPAN TAHUN INI
SELECT kd_propinsi , kd_dati2 , kd_kecamatan , kd_kelurahan, 
       count(*) as c_sppt, sum(pbb_yg_harus_dibayar_sppt)c_rp_sppt, 0 c_stts, 0 c_rp_stts, 
       0 l_sppt, 0 l_rp_sppt, 0 l_stts, 0 l_rp_stts 
FROM pbb.sppt
WHERE thn_pajak_sppt='2013'
      AND status_pembayaran_sppt<'2'
GROUP BY kd_propinsi , kd_dati2 , kd_kecamatan , kd_kelurahan

UNION
--REALISASI DAN KETETAPAN TAHUN INI
SELECT kd_propinsi , kd_dati2 , kd_kecamatan , kd_kelurahan, 
       0 as c_sppt, 0 c_rp_sppt, count(*) c_stts, sum(jml_sppt_yg_dibayar - coalesce(denda_sppt,0)) c_rp_stts, 0 l_sppt, 0 l_rp_sppt, 0 l_stts, 0 l_rp_stts 
FROM pbb.pembayaran_sppt
WHERE thn_pajak_sppt='2013'
GROUP BY kd_propinsi , kd_dati2 , kd_kecamatan , kd_kelurahan

UNION
--KETETAPAN TAHUN LALU YANG MASIH NUNGGAK
SELECT kd_propinsi , kd_dati2 , kd_kecamatan , kd_kelurahan, 0 as c_sppt, 0 c_rp_sppt, 0 c_stts, 0 c_rp_stts, 
       count(*)  l_sppt, sum(pbb_yg_harus_dibayar_sppt)  l_rp_sppt, 0 l_stts, 0 l_rp_stts 
FROM pbb.sppt
WHERE thn_pajak_sppt<'2013'
      AND status_pembayaran_sppt='0'
GROUP BY kd_propinsi , kd_dati2 , kd_kecamatan , kd_kelurahan

UNION
---REALISASI ATAS KETETAPAN TAHUN LALU DITAMBAH KETETAPANNYA UNTUK DIJUMLAHKAN SEBAGAO PIUTANG AWAL
SELECT kd_propinsi , kd_dati2 , kd_kecamatan , kd_kelurahan, 0 as c_sppt, 0 c_rp_sppt, 0 c_stts, 0  c_rp_stts, 
       count(*)  l_sppt, sum(jml_sppt_yg_dibayar - coalesce(denda_sppt,0)) l_rp_sppt, count(*) l_stts, sum(jml_sppt_yg_dibayar - coalesce(denda_sppt,0)) l_rp_stts 
FROM pbb.pembayaran_sppt
WHERE thn_pajak_sppt<'2013'
      AND tgl_pembayaran_sppt>='01-01-2013'
GROUP BY kd_propinsi , kd_dati2 , kd_kecamatan , kd_kelurahan
) AS DRV
GROUP BY 1,2,3
) AS DRV2
INNER JOIN ref_kecamatan k2
          ON  k2.kd_propinsi = drv2.kd_propinsi 
              and k2.kd_dati2 = drv2.kd_dati2
              and k2.kd_kecamatan = drv2.kd_kecamatan
ORDER BY 13 DESC LIMIT 10;;
--ORDER BY 1,2,3,4 DESC
