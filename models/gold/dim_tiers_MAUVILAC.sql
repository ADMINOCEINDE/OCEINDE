{{ config(materialized='table', schema='GOLD') }}

SELECT
    IDCONO_code_societe AS code_societe,
    IDSUNO_code_tiers AS numero_tiers,
    IDSUNO_code_tiers AS code_tiers,
    IDSUNM_nom_tiers AS libelle_tiers,
    IDSUNM_nom_tiers AS libelle_tiers_court
FROM {{ ref('cidmas_tiers') }}
WHERE IDCONO_code_societe = '200'