{{ config(materialized='table', schema='SILVER') }}

SELECT
    MWCONO AS MWCONO_code_societe,
    RTRIM(MWDIVI) AS MWDIVI_code_division,
    RTRIM(MWFACI) AS MWFACI_code_site,
    RTRIM(MWWHLO) AS MWWHLO_code_magasin,
    RTRIM(MWWHNM) AS MWWHNM_nom_magasin
FROM {{ source('db_test', 'MITWHL') }}