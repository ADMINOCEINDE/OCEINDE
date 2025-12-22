{{ config(materialized='table', schema='GOLD') }}

-- ============================================================================
-- DIM_CLIENTCPT_MAUVILAC - Dimension Client Comptant (ventes au comptoir)
-- Source: OCUSMA (silver_ocusma) + CSYTAB (libellés secteur géographique)
-- Note: Client comptant = client de passage pour ventes au comptoir
-- ============================================================================

WITH ocusma AS (
    SELECT * FROM {{ ref('ocusma_clients') }}
),

-- Secteur géographique
secteur_geo AS (
    SELECT * FROM {{ ref('csytab_parametres_systeme') }}
    WHERE CTSTCO_type_table = 'SDST'
),

-- Représentant
representant AS (
    SELECT * FROM {{ ref('csytab_parametres_systeme') }}
    WHERE CTSTCO_type_table = 'SMCD'
),

-- Groupe client
groupe_client AS (
    SELECT * FROM {{ ref('csytab_parametres_systeme') }}
    WHERE CTSTCO_type_table = 'CUCL'
)

SELECT
    -- ========================================================================
    -- IDENTIFIANTS
    -- ========================================================================

    ocusma.OKCONO_code_societe AS CSC_SOCIETE_CONO,
    201 AS CSC_SOCIETE_CODE,
    ocusma.OKCUNO_code_client AS CSC_CLIENT_CODE,

    -- ========================================================================
    -- INFORMATIONS CLIENT
    -- ========================================================================

    ocusma.OKCUNM_nom_client AS CSC_CLIENT_NOM,
    ocusma.OKSTAT_statut AS CSC_CLIENT_STATUT,

    -- ========================================================================
    -- CLASSIFICATION CLIENT
    -- ========================================================================

    -- Groupe client
    ocusma.OKCFC3_groupe_client_code AS CSC_GROUPE_CLIENT_CODE,
    groupe_client.CTTX40_libelle AS CSC_GROUPE_CLIENT_LIB,

    -- Sous-groupe client
    ocusma.OKCFC4_champ_libre_4 AS CSC_SOUSGROUPE_CLIENT_CODE,

    -- Groupement (enseigne, centrale...)
    ocusma.OKCFC8_groupement_code AS CSC_GROUPEMENT_CODE,

    -- Famille client comptant
    ocusma.OKBGRP_groupe_budget AS CSC_FAMILLE_RFA_CODE,

    -- ========================================================================
    -- GEOGRAPHIE
    -- ========================================================================

    -- Secteur géographique
    ocusma.OKECAR_code_departement AS CSC_DEPARTEMENT_CODE,
    secteur_geo.CTTX40_libelle AS CSC_SECTEUR_GEO_LIB,

    -- ========================================================================
    -- COMMERCIAL
    -- ========================================================================

    -- Représentant / Agent commercial
    representant.CTTX40_libelle AS CSC_REPRESENTANT_NOM,

    -- ========================================================================
    -- INDICATEURS
    -- ========================================================================

    CASE
        WHEN ocusma.OKSTAT_statut IN ('20', '30') THEN 1
        ELSE 0
    END AS CSC_FLAG_ACTIF

FROM ocusma

LEFT JOIN secteur_geo
    ON ocusma.OKCONO_code_societe = secteur_geo.CTCONO_code_societe
    AND ocusma.OKECAR_code_departement = secteur_geo.CTSTKY_cle

LEFT JOIN representant
    ON ocusma.OKCONO_code_societe = representant.CTCONO_code_societe

LEFT JOIN groupe_client
    ON ocusma.OKCONO_code_societe = groupe_client.CTCONO_code_societe
    AND ocusma.OKCFC3_groupe_client_code = groupe_client.CTSTKY_cle

WHERE ocusma.OKCONO_code_societe = 200