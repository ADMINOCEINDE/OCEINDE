{{ config(materialized='table', schema='GOLD') }}

SELECT
    CCDIVI_code_division AS SOC_Societe_Code,
    CCCONM_nom_societe AS SOC_Societe_Lib,
    CCCONO_code_societe_cono AS SOC_Societe_Cono
FROM {{ ref('cmndiv_divisions') }}
WHERE CCCONO_code_societe_cono = '200'