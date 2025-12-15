{{ config(materialized='table', schema='GOLD') }}

SELECT
    CKCONO_code_societe AS code_societe,
    CKECAR_code_departement AS code_departement,
    CKTW15_libelle_departement AS libelle_departement
FROM {{ ref('csysts_departements') }}
WHERE CKCONO_code_societe = '200'