{{ config(materialized='table', schema='GOLD') }}

SELECT
    CTCONO_code_societe AS SGE_Societe_Code,
    CTSTKY_cle AS SGE_Secteur_Code,
    CTTX40_libelle AS SGE_Secteur_Lib
FROM {{ ref('csytab_parametres_systeme') }}
WHERE CTCONO_code_societe = '200'
  AND CTSTCO_type_table = 'SDST'