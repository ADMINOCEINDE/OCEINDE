{{ config(materialized='table', schema='SILVER') }}

SELECT
    HICONO AS HICONO_code_societe,
    RTRIM(HIHLVL) AS HIHLVL_niveau_hierarchie,
    RTRIM(HIHIE0) AS HIHIE0_code_hierarchie,
    RTRIM(HITX40) AS HITX40_libelle_hierarchie,
    RTRIM(HITX15) AS HITX15_libelle_court
FROM {{ source('db_test', 'MITHRY') }}