{{ config(materialized='table', schema='GOLD') }}

SELECT
    CTCONO_code_societe AS PAY_SOCIETE_CODE,
    CTSTKY_cle AS PAY_PAYS_CODE,
    CTTX40_libelle AS PAY_PAYS_LIB
FROM {{ ref('csytab_parametres_systeme') }}
WHERE CTCONO_code_societe = '200'
  AND CTSTCO_type_table = 'CSCD'