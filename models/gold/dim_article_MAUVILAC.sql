{{ config(materialized='table', schema='GOLD') }}

-- ============================================================================
-- DIM_ARTICLE_MAUVILAC - Dimension Article
-- Source: MITMAS (silver_mitmas) + MITHRY (hiérarchies) + CSYTAB (libellés)
-- ============================================================================

WITH mitmas AS (
    SELECT * FROM {{ ref('mitmas_articles') }}
),

-- Hiérarchie Stats Niveau 1 - Ligne
hier_n1 AS (
    SELECT * FROM {{ ref('mithry_hierarchies_articles') }}
    WHERE HIHLVL_niveau_hierarchie = '1'
),

-- Hiérarchie Stats Niveau 2 - Gamme
hier_n2 AS (
    SELECT * FROM {{ ref('mithry_hierarchies_articles') }}
    WHERE HIHLVL_niveau_hierarchie = '2'
),

-- Hiérarchie Stats Niveau 3 - Groupe
hier_n3 AS (
    SELECT * FROM {{ ref('mithry_hierarchies_articles') }}
    WHERE HIHLVL_niveau_hierarchie = '3'
),

-- Hiérarchie Stats Niveau 4 - Sous-groupe
hier_n4 AS (
    SELECT * FROM {{ ref('mithry_hierarchies_articles') }}
    WHERE HIHLVL_niveau_hierarchie = '4'
),

-- Hiérarchie Stats Niveau 5 - Famille
hier_n5 AS (
    SELECT * FROM {{ ref('mithry_hierarchies_articles') }}
    WHERE HIHLVL_niveau_hierarchie = '5'
),

-- Libellés activité (BUAR)
activite AS (
    SELECT * FROM {{ ref('csytab_parametres_systeme') }}
    WHERE CTSTCO_type_table = 'BUAR'
),

-- Libellés groupe article (ITGR)
groupe AS (
    SELECT * FROM {{ ref('csytab_parametres_systeme') }}
    WHERE CTSTCO_type_table = 'ITGR'
)

SELECT
    -- ========================================================================
    -- IDENTIFIANTS
    -- ========================================================================

    mitmas.MMCONO_code_societe AS ART_SOCIETE_CONO,
    201 AS ART_SOCIETE_CODE,
    mitmas.MMITNO_code_article AS ART_ARTICLE_CODE,

    -- ========================================================================
    -- LIBELLES ARTICLE
    -- ========================================================================

    mitmas.MMITDS_libelle_court AS ART_LIB_COURT,
    mitmas.MMFUDS_libelle_long AS ART_LIB_LONG,

    -- ========================================================================
    -- CARACTERISTIQUES ARTICLE
    -- ========================================================================

    mitmas.MMCFI1_coloris AS ART_COLORIS,
    mitmas.MMCFI3_unite_conditionnement AS ART_UNITE_COND,
    mitmas.MMCFI4_quantite_conditionnement AS ART_QTE_COND,
    mitmas.MMUNMS_code_unite_gestion AS ART_UNITE_GESTION_CODE,

    -- ========================================================================
    -- ACTIVITE / SECTEUR
    -- ========================================================================

    mitmas.MMBUAR_code_activite AS ART_ACTIVITE_CODE,
    activite.CTTX40_libelle AS ART_ACTIVITE_LIB,

    -- ========================================================================
    -- GROUPE ARTICLE
    -- ========================================================================

    mitmas.MMITGR_code_groupe AS ART_GROUPE_CODE,
    groupe.CTTX40_libelle AS ART_GROUPE_LIB,

    -- ========================================================================
    -- HIERARCHIE STATISTIQUE (5 niveaux)
    -- ========================================================================

    -- Niveau 1 - Ligne
    mitmas.MMHIE1_hier_stats_n1_ligne_code AS ART_HIER_STATS_N1_LIG_CODE,
    hier_n1.HITX40_libelle_hierarchie AS ART_HIER_STATS_N1_LIG_LIB,

    -- Niveau 2 - Gamme
    mitmas.MMHIE2_hier_stats_n2_gamme_code AS ART_HIER_STATS_N2_GAMME_CODE,
    hier_n2.HITX40_libelle_hierarchie AS ART_HIER_STATS_N2_GAMME_LIB,

    -- Niveau 3 - Groupe
    mitmas.MMHIE3_hier_stats_n3_groupe_code AS ART_HIER_STATS_N3_GROUPE_CODE,
    hier_n3.HITX40_libelle_hierarchie AS ART_HIER_STATS_N3_GROUPE_LIB,

    -- Niveau 4 - Sous-groupe
    mitmas.MMHIE4_hier_stats_n4_sousgroupe_code AS ART_HIER_STATS_N4_SOUSGROUPE_CODE,
    hier_n4.HITX40_libelle_hierarchie AS ART_HIER_STATS_N4_SOUSGROUPE_LIB,

    -- Niveau 5 - Famille
    mitmas.MMHIE5_hier_stats_n5_famille_code AS ART_HIER_STATS_N5_FAMILLE_CODE,
    hier_n5.HITX40_libelle_hierarchie AS ART_HIER_STATS_N5_FAMILLE_LIB

FROM mitmas

LEFT JOIN activite
    ON mitmas.MMCONO_code_societe = activite.CTCONO_code_societe
    AND mitmas.MMBUAR_code_activite = activite.CTSTKY_cle

LEFT JOIN groupe
    ON mitmas.MMCONO_code_societe = groupe.CTCONO_code_societe
    AND mitmas.MMITGR_code_groupe = groupe.CTSTKY_cle

LEFT JOIN hier_n1
    ON mitmas.MMCONO_code_societe = hier_n1.HICONO_code_societe
    AND mitmas.MMHIE1_hier_stats_n1_ligne_code = hier_n1.HIHIE0_code_hierarchie

LEFT JOIN hier_n2
    ON mitmas.MMCONO_code_societe = hier_n2.HICONO_code_societe
    AND mitmas.MMHIE2_hier_stats_n2_gamme_code = hier_n2.HIHIE0_code_hierarchie

LEFT JOIN hier_n3
    ON mitmas.MMCONO_code_societe = hier_n3.HICONO_code_societe
    AND mitmas.MMHIE3_hier_stats_n3_groupe_code = hier_n3.HIHIE0_code_hierarchie

LEFT JOIN hier_n4
    ON mitmas.MMCONO_code_societe = hier_n4.HICONO_code_societe
    AND mitmas.MMHIE4_hier_stats_n4_sousgroupe_code = hier_n4.HIHIE0_code_hierarchie

LEFT JOIN hier_n5
    ON mitmas.MMCONO_code_societe = hier_n5.HICONO_code_societe
    AND mitmas.MMHIE5_hier_stats_n5_famille_code = hier_n5.HIHIE0_code_hierarchie

WHERE mitmas.MMCONO_code_societe = 200