{{ config(materialized='table', schema='GOLD') }}

SELECT
    MWDIVI_code_division AS MAG_Societe_Code,
    MWWHLO_code_magasin AS MAG_Magasin_Code,
    MWWHNM_nom_magasin AS MAG_Magasin_Lib
FROM {{ ref('mitwhl_magasins') }}
WHERE MWCONO_code_societe = '200'