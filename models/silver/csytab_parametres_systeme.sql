{{ config(materialized='table', schema='SILVER') }}

SELECT
    CTCONO AS CTCONO_code_societe,
    RTRIM(CTSTCO) AS CTSTCO_type_table,
    RTRIM(CTSTKY) AS CTSTKY_cle,
    RTRIM(CTPARM) AS CTPARM_parametre,
    RTRIM(CTTX40) AS CTTX40_libelle
FROM {{ source('db2i_hva_m3fdbprd', 'CSYTAB') }}