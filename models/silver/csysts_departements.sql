{{ config(materialized='table', schema='SILVER') }}

SELECT
    CKCONO AS CKCONO_code_societe,
    RTRIM(CKECAR) AS CKECAR_code_departement,
    RTRIM(CKTX15) AS CKTW15_libelle_departement
FROM {{ source('db2i_hva_m3fdbprd', 'CSYSTS') }}