{{ config(materialized='table', schema='GOLD') }}

SELECT
    MWFACI_code_site AS DEP_Societe_Code,
    MWWHLO_code_entrepot AS DEP_Depot_Code,
    MWWHNM_nom_entrepot AS DEP_Depot_Lib
FROM {{ ref('mitwhl_entrepots') }}
WHERE MWCONO_code_societe = '200'