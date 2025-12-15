{{ config(materialized='table', schema='SILVER') }}

SELECT
    RTRIM(JUUSID) AS JUUSID_id_utilisateur,
    JUDFCO AS JUDFCO_code_societe_defaut,
    JUDFDI AS JUDFDI_code_division_defaut
FROM {{ source('db2i_hva_m3fdbprd', 'CMNUSR') }}