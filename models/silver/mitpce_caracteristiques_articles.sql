{{ config(materialized='table', schema='SILVER') }}

SELECT
    MSCONO AS MSCONO_code_societe,
    RTRIM(MSWHSL) AS MSWHSL_code_qualite,
    RTRIM(MSSLDS) AS MSSLDS_libelle_qualite
FROM {{ source('db2i_hva_m3fdbprd', 'MITPCE') }}