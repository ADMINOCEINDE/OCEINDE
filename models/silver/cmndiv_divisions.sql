{{ config(materialized='table', schema='SILVER') }}

SELECT
    CCCONO AS CCCONO_code_societe_cono,
    RTRIM(CCDIVI) AS CCDIVI_code_division,
    RTRIM(CCCONM) AS CCCONM_nom_societe
FROM {{ source('db2i_hva_m3fdbprd', 'CMNDIV') }}