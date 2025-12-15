{{ config(materialized='table', schema='SILVER') }}

SELECT
    MWCONO AS MWCONO_code_societe,
    RTRIM(MWFACI) AS MWFACI_code_site,
    RTRIM(MWWHLO) AS MWWHLO_code_entrepot,
    RTRIM(MWWHNM) AS MWWHNM_nom_entrepot
FROM {{ source('db2i_hva_m3fdbprd', 'MITWHL') }}