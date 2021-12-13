package org.novakorp.gp.tester

object Queries {

  lazy val q_audiencias_clientes_ber: String = {
    """ SELECT DISTINCT
               ch.sexo
             , ch.estado_civil
             , CASE
                 WHEN SUBSTR(CAST(ch.fecha_nacimiento AS STRING), 1, 4) BETWEEN '1997' AND '2012' THEN
                   'Gen Z'
                 WHEN SUBSTR(CAST(ch.fecha_nacimiento AS STRING), 1, 4) BETWEEN '1981' AND '1996' THEN
                   'Millenials'
                 WHEN SUBSTR(CAST(ch.fecha_nacimiento AS STRING), 1, 4) BETWEEN '1973' AND '1980' THEN
                   'Late Gen X'
                 WHEN SUBSTR(CAST(ch.fecha_nacimiento AS STRING), 1, 4) BETWEEN '1965' AND '1973' THEN
                   'Early Gen x'
                 WHEN SUBSTR(CAST(ch.fecha_nacimiento AS STRING), 1, 4) BETWEEN '1955' AND '1964' THEN
                   'Late baby boomers'
                 WHEN SUBSTR(CAST(ch.fecha_nacimiento AS STRING), 1, 4) BETWEEN '1946' AND '1954' THEN
                   'Early baby boomers'
                 ELSE
                   'Otros'
               END AS generacion
             , NVL(ch.nro_cuit, ch.nro_cuil) nro_cuit_cuil
             , ch.monto_presunto
             , fra.producto_ofrecido
             , CASE
                 WHEN fra.resultado_campania = 2 THEN
                   'Rechazo la oferta'
                 WHEN fra.resultado_campania = 1 THEN
                   'Mostro interes'
                 ELSE
                   'Sin accion'
               END resultado_campania
             , fra.producto_adquirido
             , fra.fecha_adquisicion
             , fra.canal_venta
             , fra.id_campania_sms
             , fra.id_campania_cc
             , fra.fecha_resultado
             , fra.id_prospecto
             , CAST(ch.id_cliente_core AS BIGINT) id_cliente_core
             , fra.id_persona
             , 'ber' as entidad
          FROM de_qua_3ref.ft_resultado_audiencias fra
             , de_ber_3ref.rel_cliente_core_protegido rccp
             , test.cliente_homologado_ber ch
         WHERE fra.id_prospecto = rccp.id_cliente_core_protegido
           AND rccp.id_cliente_core = ch.id_cliente_core
           AND fra.canal = 'SMS' """
  }

  lazy val q_audiencias_clientes_bsf: String = {
    """ SELECT DISTINCT
               ch.sexo
             , ch.estado_civil
             , CASE
                 WHEN SUBSTR(CAST(ch.fecha_nacimiento AS STRING), 1, 4) BETWEEN '1997' AND '2012' THEN
                   'Gen Z'
                 WHEN SUBSTR(CAST(ch.fecha_nacimiento AS STRING), 1, 4) BETWEEN '1981' AND '1996' THEN
                   'Millenials'
                 WHEN SUBSTR(CAST(ch.fecha_nacimiento AS STRING), 1, 4) BETWEEN '1973' AND '1980' THEN
                   'Late Gen X'
                 WHEN SUBSTR(CAST(ch.fecha_nacimiento AS STRING), 1, 4) BETWEEN '1965' AND '1973' THEN
                   'Early Gen x'
                 WHEN SUBSTR(CAST(ch.fecha_nacimiento AS STRING), 1, 4) BETWEEN '1955' AND '1964' THEN
                   'Late baby boomers'
                 WHEN SUBSTR(CAST(ch.fecha_nacimiento AS STRING), 1, 4) BETWEEN '1946' AND '1954' THEN
                   'Early baby boomers'
                 ELSE
                   'Otros'
               END AS generacion
             , NVL(ch.nro_cuit, ch.nro_cuil) nro_cuit_cuil
             , ch.monto_presunto
             , fra.producto_ofrecido
             , CASE
                 WHEN fra.resultado_campania = 2 THEN
                   'Rechazo la oferta'
                 WHEN fra.resultado_campania = 1 THEN
                   'Mostro interes'
                 ELSE
                   'Sin accion'
               END resultado_campania
             , fra.producto_adquirido
             , fra.fecha_adquisicion
             , fra.canal_venta
             , fra.id_campania_sms
             , fra.id_campania_cc
             , fra.fecha_resultado
             , fra.id_prospecto
             , CAST(ch.id_cliente_core AS BIGINT) id_cliente_core
             , fra.id_persona
             , 'bsf' as entidad
          FROM de_qua_3ref.ft_resultado_audiencias fra
             , de_bsf_3ref.rel_cliente_core_protegido rccp
             , test.cliente_homologado_bsf ch
         WHERE fra.id_prospecto = rccp.id_cliente_core_protegido
           AND rccp.id_cliente_core = ch.id_cliente_core
           AND fra.canal = 'SMS' """
  }

  lazy val q_bcra_riesgo: String = {
    """ WITH cli_max_per_inf AS ( SELECT id_persona
                                       , MAX(periodo_informado) pinf
                                    FROM de_gpn_2cur.bcra_deudores_24m
                                GROUP BY id_persona ),
        cli_max_sit_mes_1 AS ( SELECT bd.id_persona
                                    , cm.pinf
                                    , MAX(bd.monto_mes_1) mm
                                 FROM de_gpn_2cur.bcra_deudores_24m bd
                           INNER JOIN cli_max_per_inf cm
                                   ON bd.id_persona = cm.id_persona
                                  AND bd.periodo_informado = cm.pinf
                             GROUP BY bd.id_persona, cm.pinf )
        SELECT DISTINCT
               CASE
                 WHEN bd.situacion_mes_1 = 0 THEN 'Bajo'
                 WHEN bd.situacion_mes_1 IN (1, 2) THEN 'Medio'
                 WHEN bd.situacion_mes_1 IN (3, 4, 5) THEN 'Alto'
               END AS riesgo_crediticio_1m
             , CASE
                 WHEN bd.situacion_mes_3 = 0 THEN 'Bajo'
                 WHEN bd.situacion_mes_3 IN (1, 2) THEN 'Medio'
                 WHEN bd.situacion_mes_3 IN (3, 4, 5) THEN 'Alto'
               END AS riesgo_crediticio_3m
             , CASE
                 WHEN bd.situacion_mes_6 = 0 THEN 'Bajo'
                 WHEN bd.situacion_mes_6 IN (1, 2) THEN 'Medio'
                 WHEN bd.situacion_mes_6 IN (3, 4, 5) THEN 'Alto'
               END AS riesgo_crediticio_6m
             , bd.id_persona
          FROM de_gpn_2cur.bcra_deudores_24m bd
    INNER JOIN cli_max_sit_mes_1 cm
            ON bd.id_persona = cm.id_persona
           AND bd.periodo_informado = cm.pinf
           AND bd.monto_mes_1 = cm.mm """
  }

  lazy val q_bcra_antiguedad: String = {
    """ WITH cli_max_per_inf AS ( SELECT id_persona
                                       , MAX(periodo_informado) pinf
                                    FROM de_gpn_2cur.bcra_deudores_24m
                                GROUP BY id_persona )
           , cli_max_ant AS ( SELECT bd.id_persona
                                   , cm.pinf
                                   , MAX(ANTIGUEDAD_SF(array(cast(NVL(bd.monto_mes_1,'0.00') as string)
                                                            ,cast(NVL(bd.monto_mes_2,'0.00') as string)
                                                            ,cast(NVL(bd.monto_mes_3,'0.00') as string)
                                                            ,cast(NVL(bd.monto_mes_4,'0.00') as string)
                                                            ,cast(NVL(bd.monto_mes_5,'0.00') as string)
                                                            ,cast(NVL(bd.monto_mes_6,'0.00') as string)
                                                            ,cast(NVL(bd.monto_mes_7,'0.00') as string)
                                                            ,cast(NVL(bd.monto_mes_8,'0.00') as string)
                                                            ,cast(NVL(bd.monto_mes_9,'0.00') as string)
                                                            ,cast(NVL(bd.monto_mes_10,'0.00') as string)
                                                            ,cast(NVL(bd.monto_mes_11,'0.00') as string)
                                                            ,cast(NVL(bd.monto_mes_12,'0.00') as string)
                                                            ,cast(NVL(bd.monto_mes_13,'0.00') as string)
                                                            ,cast(NVL(bd.monto_mes_14,'0.00') as string)
                                                            ,cast(NVL(bd.monto_mes_15,'0.00') as string)
                                                            ,cast(NVL(bd.monto_mes_16,'0.00') as string)
                                                            ,cast(NVL(bd.monto_mes_17,'0.00') as string)
                                                            ,cast(NVL(bd.monto_mes_18,'0.00') as string)
                                                            ,cast(NVL(bd.monto_mes_19,'0.00') as string)
                                                            ,cast(NVL(bd.monto_mes_20,'0.00') as string)
                                                            ,cast(NVL(bd.monto_mes_21,'0.00') as string)
                                                            ,cast(NVL(bd.monto_mes_22,'0.00') as string)
                                                            ,cast(NVL(bd.monto_mes_23,'0.00') as string)
                                                            ,cast(NVL(bd.monto_mes_24,'0.00') as string)))) AS max_ant
                                FROM de_gpn_2cur.bcra_deudores_24m bd
                          INNER JOIN cli_max_per_inf cm
                                  ON bd.id_persona = cm.id_persona
                                 AND bd.periodo_informado = cm.pinf
                            GROUP BY bd.id_persona
                                   , cm.pinf )
        SELECT DISTINCT
               CASE
                 WHEN cm.max_ant >= 0 AND cm.max_ant <= 6 THEN '0-6 m'
                 WHEN cm.max_ant >= 7 AND cm.max_ant <= 12 THEN '6-12 m'
                 WHEN cm.max_ant >= 13 AND cm.max_ant <= 18 THEN '12-18 m'
                 ELSE '+18 m'
               END AS antiguedad_sistema_financiero
             , bd.id_persona
          FROM de_gpn_2cur.bcra_deudores_24m bd
    INNER JOIN cli_max_ant cm
            ON bd.id_persona = cm.id_persona
           AND bd.periodo_informado = cm.pinf """
  }

  lazy val consumos_ber: String = {
    """ SELECT id_cliente_core
             , fecha_transaccion
             , mto_transaccion
             , id_categoria
          FROM de_ber_3ref.ft_nbo_qualia_categorias_consumo """
  }

  lazy val consumos_bsf: String = {
    """ SELECT id_cliente_core
             , fecha_transaccion
             , mto_transaccion
             , id_categoria
          FROM de_bsf_3ref.ft_nbo_qualia_categorias_consumo """
  }

  lazy val fecha_campania: String = {
    " SELECT max(fecha_resultado) FROM de_qua_3ref.ft_resultado_audiencias "
  }

}
