{{ config(materialized='table', schema='SILVER') }}

SELECT
    MMCONO AS MMCONO_code_societe,
    RTRIM(MMITNO) AS MMITNO_code_article,
    RTRIM(MMFUDS) AS MMFUDS_libelle_long,
    RTRIM(MMITDS) AS MMITDS_libelle_court,
    RTRIM(MMCFI1) AS MMCFI1_coloris,
    RTRIM(MMCFI4) AS MMCFI4_quantite_conditionnement,
    RTRIM(MMCFI3) AS MMCFI3_unite_conditionnement,
    RTRIM(MMBUAR) AS MMBUAR_code_activite,
    RTRIM(MMITGR) AS MMITGR_code_groupe,
    RTRIM(MMHIE1) AS MMHIE1_hier_stats_n1_ligne_code,
    RTRIM(MMHIE2) AS MMHIE2_hier_stats_n2_gamme_code,
    RTRIM(MMHIE3) AS MMHIE3_hier_stats_n3_groupe_code,
    RTRIM(MMHIE4) AS MMHIE4_hier_stats_n4_sousgroupe_code,
    RTRIM(MMHIE5) AS MMHIE5_hier_stats_n5_famille_code,
    RTRIM(MMUNMS) AS MMUNMS_code_unite_gestion
FROM {{ source('db2i_hva_m3fdbprd', 'MITMAS') }}