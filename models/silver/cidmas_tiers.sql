{{ config(materialized='table', schema='SILVER') }}

SELECT
    IDCONO AS IDCONO_code_societe,
    RTRIM(IDSUNO) AS IDSUNO_code_tiers,
    RTRIM(IDSUNM) AS IDSUNM_nom_tiers
FROM {{ source('db2i_hva_m3fdbprd', 'CIDMAS') }}