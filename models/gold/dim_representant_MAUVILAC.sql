{{ config(materialized='table', schema='GOLD') }}

WITH parametres AS (
    SELECT *
    FROM {{ ref('csytab_parametres_systeme') }}
),

utilisateurs AS (
    SELECT *
    FROM {{ ref('cmnusr_utilisateurs') }}
)

SELECT
    p.CTCONO_code_societe AS REP_Societe_Code,
    
    -- INFO UTILISATEUR
    u.JUDFDI_code_division_defaut AS REP_division,
    
    -- TYPE REPRÉSENTANT
    SUBSTRING(p.CTPARM_parametre, 1, 3) AS REP_Type_Rep_Code,
    t.CTTX40_libelle AS REP_Type_Rep_Lib,
    
    -- REPRÉSENTANT
    p.CTSTKY_cle AS REP_Representant_Code,
    p.CTTX40_libelle AS REP_Representant_Nom

FROM parametres p

LEFT JOIN parametres t
    ON p.CTCONO_code_societe = t.CTCONO_code_societe
   AND SUBSTRING(p.CTPARM_parametre, 1, 3) = t.CTSTKY_cle
   AND t.CTSTCO_type_table = 'SDEP'

LEFT JOIN utilisateurs u
    ON u.JUUSID_id_utilisateur = p.CTSTKY_cle
   AND p.CTCONO_code_societe = u.JUDFCO_code_societe_defaut

WHERE p.CTCONO_code_societe = '200'
  AND p.CTSTCO_type_table = 'SMCD'