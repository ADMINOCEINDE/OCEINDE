{{ config(materialized='table', schema='GOLD') }}

-- ============================================================================
-- FAIT_SJCC_MAUVILAC - Table de faits des ventes Client Comptant
-- Stats Journalières Client Comptant (agrégation par jour/article/client/dépôt)
-- Source: OSBSTD (osbstd_stats_ventes) + OOLINE (ooline_lignes_commandes)
-- ============================================================================

WITH osbstd AS (
    SELECT * FROM {{ ref('osbstd_stats_ventes') }}
),

ooline AS (
    SELECT * FROM {{ ref('ooline_lignes_commandes') }}
)

SELECT
    -- ========================================================================
    -- CLE COMPOSITE STATISTIQUE
    -- ========================================================================

    CONCAT(
        osbstd.UCDIVI_code_division, '-',
        osbstd.UCITNO_code_article, '-',
        osbstd.UCSMCD_code_representant, '-',
        osbstd.UCWHLO_code_depot, '-',
        YEAR(TO_DATE(CAST(osbstd.UCORDT_date_commande AS STRING), 'yyyyMMdd')), '-',
        LPAD(MONTH(TO_DATE(CAST(osbstd.UCORDT_date_commande AS STRING), 'yyyyMMdd')), 2, '0'), '-',
        LPAD(DAY(TO_DATE(CAST(osbstd.UCORDT_date_commande AS STRING), 'yyyyMMdd')), 2, '0')
    ) AS VCC_STATCC_CODE,

    -- ========================================================================
    -- DIMENSIONS
    -- ========================================================================

    -- Société
    osbstd.UCCONO_code_societe AS VCC_SOCIETE_CONO,
    osbstd.UCDIVI_code_division AS VCC_CODE_SOCIETE,

    -- Calendrier
    YEAR(TO_DATE(CAST(osbstd.UCORDT_date_commande AS STRING), 'yyyyMMdd')) AS VCC_ANNEE,
    MONTH(TO_DATE(CAST(osbstd.UCORDT_date_commande AS STRING), 'yyyyMMdd')) AS VCC_MOIS,
    DAY(TO_DATE(CAST(osbstd.UCORDT_date_commande AS STRING), 'yyyyMMdd')) AS VCC_JOUR,
    TO_DATE(CAST(osbstd.UCORDT_date_commande AS STRING), 'yyyyMMdd') AS VCC_DATE,

    -- Article
    osbstd.UCITNO_code_article AS VCC_ARTICLE_CODE,

    -- Client (client comptant = client de passage)
    osbstd.UCCUNO_code_client_commande AS VCC_CLIENT_CODE,

    -- Dépôt
    osbstd.UCWHLO_code_depot AS VCC_DEPOT_CODE,

    -- Représentant / Agent commercial
    osbstd.UCSMCD_code_representant AS VCC_REPRESENTANT_CODE,

    -- ========================================================================
    -- MESURES - QUANTITES
    -- ========================================================================

    -- Quantité nette (facturée)
    SUM(osbstd.UCIVQT_quantite_facturee_um) AS VCC_QTE_NETTE,

    -- Quantités en unités secondaires (KG et L)
    SUM(osbstd.UCIVQT_quantite_facturee_um * COALESCE(osbstd.UCGRWE_poids_brut, 0)) AS VCC_QTE_NETTE_UNI_SEC_1,
    SUM(osbstd.UCVOL3_volume) AS VCC_QTE_NETTE_UNI_SEC_2,

    -- Codes unités secondaires
    'KG' AS VCC_UNI_SEC_1_CODE,
    'L' AS VCC_UNI_SEC_2_CODE,

    -- ========================================================================
    -- MESURES - CHIFFRE D'AFFAIRES
    -- ========================================================================

    -- CA HT Net
    SUM(osbstd.UCSAAM_montant_net_ligne) AS VCC_CA_HT_NET,

    -- CA HT Brut
    SUM(ooline.OBLNAM_montant_ligne_devise_commande) AS VCC_CA_HT_BRUT,

    -- ========================================================================
    -- MESURES - REMISES AGREGEES
    -- ========================================================================

    SUM(osbstd.UCDIA1_montant_remise_1) AS VCC_MTT_REMISE_1,
    SUM(osbstd.UCDIA2_montant_remise_2) AS VCC_MTT_REMISE_2,
    SUM(osbstd.UCDIA3_montant_remise_3) AS VCC_MTT_REMISE_3,
    SUM(osbstd.UCDIA4_montant_remise_4) AS VCC_MTT_REMISE_4,
    SUM(osbstd.UCDIA5_montant_remise_5) AS VCC_MTT_REMISE_5,
    SUM(osbstd.UCDIA6_montant_remise_6) AS VCC_MTT_REMISE_6,
    SUM(osbstd.UCOFRA_total_montant_remise) AS VCC_MTT_REMISE_TOTAL,

    -- ========================================================================
    -- MESURES - COUTS ET MARGE
    -- ========================================================================

    SUM(osbstd.UCDCOS_cout_revient_livraison) AS VCC_COUT_REVIENT,
    SUM(osbstd.UCSAAM_montant_net_ligne - osbstd.UCDCOS_cout_revient_livraison) AS VCC_MARGE_BRUTE,

    -- ========================================================================
    -- MESURES - COMPTEURS
    -- ========================================================================

    COUNT(*) AS VCC_NB_LIGNES,
    COUNT(DISTINCT osbstd.UCORNO_numero_commande_client) AS VCC_NB_COMMANDES,

    -- ========================================================================
    -- AUDIT
    -- ========================================================================

    MAX(osbstd.UCLMDT_date_modification) AS VCC_DATE_MAJ,
    CURRENT_TIMESTAMP() AS VCC_DATE_EXTRACTION

FROM osbstd

LEFT JOIN ooline
    ON osbstd.UCCONO_code_societe = ooline.OBCONO_code_societe
    AND osbstd.UCORNO_numero_commande_client = ooline.OBORNO_numero_commande_client
    AND osbstd.UCPONR_numero_ligne_commande = ooline.OBPONR_numero_ligne_commande
    AND osbstd.UCPOSX_suffixe_ligne = ooline.OBPOSX_suffixe_ligne

WHERE osbstd.UCCONO_code_societe = 200

GROUP BY
    osbstd.UCCONO_code_societe,
    osbstd.UCDIVI_code_division,
    TO_DATE(CAST(osbstd.UCORDT_date_commande AS STRING), 'yyyyMMdd'),
    osbstd.UCITNO_code_article,
    osbstd.UCCUNO_code_client_commande,
    osbstd.UCWHLO_code_depot,
    osbstd.UCSMCD_code_representant
