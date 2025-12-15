{{ config(materialized='table', schema='GOLD') }}

SELECT DISTINCT
    201 AS QLT_SOCIETE_CODE,
    MSWHSL_code_qualite AS QLT_CODE,
    MSSLDS_libelle_qualite AS QLT_LIBELLE
FROM {{ ref('mitpce_caracteristiques_articles') }}
WHERE MSCONO_code_societe = '200'