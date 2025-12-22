{{ config(materialized='table', schema='GOLD') }}

-- ============================================================================
-- FAIT_VENTES_SJCA_MAUVILAC - Table de faits des ventes (Stats Journalières CA)
-- Source: OSBSTD (osbstd_stats_ventes) + OOLINE (ooline_lignes_commandes) + OCUSMA (ocusma_clients)
-- ============================================================================

WITH osbstd AS (
    SELECT * FROM {{ ref('osbstd_stats_ventes') }}
),

ooline AS (
    SELECT * FROM {{ ref('ooline_lignes_commandes') }}
),

ocusma AS (
    SELECT * FROM {{ ref('ocusma_clients') }}
)

SELECT
    -- ========================================================================
    -- CLES ET DIMENSIONS
    -- ========================================================================

    -- Identifiants
    osbstd.UCCONO_code_societe AS VTE_SOCIETE_CONO,
    osbstd.UCDIVI_code_division AS VTE_SOCIETE_CODE,
    osbstd.UCORNO_numero_commande_client AS VTE_COMMANDE_CODE,
    osbstd.UCPONR_numero_ligne_commande AS VTE_LIGNE_COMMANDE,
    osbstd.UCPOSX_suffixe_ligne AS VTE_SUFFIXE_LIGNE,

    -- Planning / Calendrier
    TO_DATE(CAST(osbstd.UCORDT_date_commande AS STRING), 'yyyyMMdd') AS VTE_DATE_COMMANDE,
    YEAR(TO_DATE(CAST(osbstd.UCORDT_date_commande AS STRING), 'yyyyMMdd')) AS VTE_ANNEE,
    MONTH(TO_DATE(CAST(osbstd.UCORDT_date_commande AS STRING), 'yyyyMMdd')) AS VTE_MOIS,
    DAY(TO_DATE(CAST(osbstd.UCORDT_date_commande AS STRING), 'yyyyMMdd')) AS VTE_JOUR,
    CONCAT(osbstd.UCDIVI_code_division, '-', YEAR(TO_DATE(CAST(osbstd.UCORDT_date_commande AS STRING), 'yyyyMMdd')), '-', LPAD(MONTH(TO_DATE(CAST(osbstd.UCORDT_date_commande AS STRING), 'yyyyMMdd')), 2, '0'), '-', LPAD(DAY(TO_DATE(CAST(osbstd.UCORDT_date_commande AS STRING), 'yyyyMMdd')), 2, '0')) AS VTE_PLANNING_CODE,

    -- Dates clés
    TO_DATE(CAST(osbstd.UCIVDT_date_facture AS STRING), 'yyyyMMdd') AS VTE_DATE_FACTURE,
    TO_DATE(CAST(osbstd.UCDWDT_date_livraison_demandee AS STRING), 'yyyyMMdd') AS VTE_DATE_LIVRAISON_DEMANDEE,
    TO_DATE(CAST(osbstd.UCCODT_date_livraison_confirmee AS STRING), 'yyyyMMdd') AS VTE_DATE_LIVRAISON_CONFIRMEE,
    TO_DATE(CAST(osbstd.UCDLDT_date_livraison_planifiee AS STRING), 'yyyyMMdd') AS VTE_DATE_LIVRAISON_PLANIFIEE,
    TO_DATE(CAST(osbstd.UCACDT_date_comptable AS STRING), 'yyyyMMdd') AS VTE_DATE_COMPTABLE,

    -- Références documents
    osbstd.UCDLIX_numero_bl_m3 AS VTE_BL_CODE,
    osbstd.UCIVNO_numero_facture AS VTE_FACTURE_CODE,
    osbstd.UCEXIN_numero_facture_etendu AS VTE_PIECE_CODE,

    -- ========================================================================
    -- DIMENSIONS GEOGRAPHIQUES ET COMMERCIALES
    -- ========================================================================

    -- Client
    osbstd.UCCUNO_code_client_commande AS VTE_CLIENT_CODE,
    osbstd.UCDECU_code_client_livre AS VTE_CLIENT_LIVRE_CODE,
    osbstd.UCINRC_code_client_facture AS VTE_CLIENT_FACTURE_CODE,
    osbstd.UCPYNO_code_client_payeur AS VTE_CLIENT_PAYEUR_CODE,
    osbstd.UCCUST_client_statistique AS VTE_CLIENT_STAT_CODE,
    osbstd.UCCUCL_groupe_client AS VTE_GROUPE_CLIENT_CODE,
    osbstd.UCCUTP_type_client AS VTE_TYPE_CLIENT_CODE,

    -- Enrichissement Client depuis OCUSMA
    ocusma.OKCFC8_groupement_code AS VTE_GROUPEMENT_CODE,
    ocusma.OKCFC3_groupe_client_code AS VTE_GROUPECLI_CODE,
    ocusma.OKECAR_code_departement AS VTE_DEPARTEMENT_CODE,

    -- Géographie
    osbstd.UCCSCD_code_pays AS VTE_PAYS_CODE,
    osbstd.UCSDST_region AS VTE_SECTEUR_CODE,

    -- Commercial
    osbstd.UCSMCD_code_representant AS VTE_REPRESENTANT_CODE,
    osbstd.UCSDEP_departement_vente AS VTE_DEPT_VENTE_CODE,

    -- Logistique
    osbstd.UCWHLO_code_depot AS VTE_DEPOT_CODE,
    osbstd.UCFACI_code_etablissement AS VTE_ETABLISSEMENT_CODE,
    osbstd.UCROUT_tournee AS VTE_TOURNEE_CODE,
    osbstd.UCRODN_depart_tournee AS VTE_DEPART_TOURNEE_CODE,

    -- ========================================================================
    -- DIMENSIONS ARTICLE
    -- ========================================================================

    osbstd.UCITNO_code_article AS VTE_ARTICLE_CODE,
    osbstd.UCREPI_code_article_remplace AS VTE_ARTICLE_REMPLACE_CODE,
    osbstd.UCITCL_groupe_produit AS VTE_GROUPE_PRODUIT_CODE,
    osbstd.UCITGR_groupe_article AS VTE_GROUPE_ARTICLE_CODE,
    osbstd.UCITTY_type_article AS VTE_TYPE_ARTICLE_CODE,
    osbstd.UCBUAR_secteur_activite AS VTE_SECTEUR_ACTIVITE_CODE,

    -- ========================================================================
    -- DIMENSIONS CAMPAGNES ET PROMOTIONS
    -- ========================================================================

    osbstd.UCPIDE_code_promotion AS VTE_PROMO_CODE,
    osbstd.UCAGNO_numero_contrat_chantier AS VTE_AFFAIRE_CODE,
    osbstd.UCCMNO_campagne_tarifaire AS VTE_CAMPAGNE_TARIF_CODE,
    osbstd.UCCMP1_campagne_remise_1 AS VTE_CAMPAGNE_REMISE_1,
    osbstd.UCCMP2_campagne_remise_2 AS VTE_CAMPAGNE_REMISE_2,
    osbstd.UCCMP3_campagne_valisette AS VTE_CAMPAGNE_VALISETTE,
    osbstd.UCCMP4_campagne_remise_4 AS VTE_CAMPAGNE_REMISE_4,
    osbstd.UCCMP5_campagne_remise_5 AS VTE_CAMPAGNE_REMISE_5,
    osbstd.UCCMP6_campagne_remise_6 AS VTE_CAMPAGNE_REMISE_6,

    -- ========================================================================
    -- MESURES - QUANTITES
    -- ========================================================================

    -- Quantités commandées
    osbstd.UCORQT_quantite_commandee_um AS VTE_QTE_COMMANDEE_UM,
    osbstd.UCORQA_quantite_commandee_um_alt AS VTE_QTE_COMMANDEE_UM_ALT,
    osbstd.UCORQS_quantite_commandee_um_prix AS VTE_QTE_COMMANDEE_UM_PRIX,
    osbstd.UCORQB_quantite_commandee_um_stat AS VTE_QTE_COMMANDEE_UM_STAT,
    osbstd.UCDEMA_quantite_demande_initiale AS VTE_QTE_DEMANDE_INITIALE,

    -- Quantités facturées
    osbstd.UCIVQT_quantite_facturee_um AS VTE_QTE_NETTE,
    osbstd.UCIVQA_quantite_facturee_um_alt AS VTE_QTE_NETTE_UM_ALT,
    osbstd.UCIVQS_quantite_facturee_um_prix AS VTE_QTE_NETTE_UM_PRIX,
    osbstd.UCOFQS_quantite_facturee_um_stat AS VTE_QTE_NETTE_UM_STAT,

    -- Quantités par type (calcul selon catégorie mouvement)
    CASE WHEN osbstd.UCORTK_categorie_mouvement = 2 THEN osbstd.UCIVQT_quantite_facturee_um ELSE 0 END AS VTE_QTE_AVOIR,
    CASE WHEN osbstd.UCPIDE_code_promotion IS NOT NULL AND osbstd.UCPIDE_code_promotion != '' THEN osbstd.UCIVQT_quantite_facturee_um ELSE 0 END AS VTE_QTE_NETTE_PROMO,
    CASE WHEN osbstd.UCAGNO_numero_contrat_chantier IS NOT NULL AND osbstd.UCAGNO_numero_contrat_chantier != '' THEN osbstd.UCIVQT_quantite_facturee_um ELSE 0 END AS VTE_QTE_NETTE_AFFAIRE,

    -- Unités
    osbstd.UCSPUN_unite_prix_vente AS VTE_UNITE_PRIX,
    osbstd.UCSTUN_unite_statistique AS VTE_UNITE_STAT,
    'KG' AS VTE_UNI_SEC_1_CODE,
    'L' AS VTE_UNI_SEC_2_CODE,

    -- ========================================================================
    -- MESURES - CHIFFRE D'AFFAIRES
    -- ========================================================================

    -- CA Net (depuis OSBSTD)
    osbstd.UCSAAM_montant_net_ligne AS VTE_CA_HT_NET,
    osbstd.UCCUAM_montant_net_devise_tiers AS VTE_CA_HT_NET_DEVISE,
    osbstd.UCSGAM_montant_prix_vente_fiche AS VTE_CA_PRIX_FICHE,

    -- CA Brut (depuis OOLINE)
    ooline.OBLNAM_montant_ligne_devise_commande AS VTE_CA_HT_BRUT,
    ooline.OBLNA2_montant_ligne_devise_societe AS VTE_CA_HT_BRUT_SOCIETE,

    -- CA par type
    CASE WHEN osbstd.UCORTK_categorie_mouvement = 2 THEN osbstd.UCSAAM_montant_net_ligne ELSE 0 END AS VTE_CA_HT_NET_AVOIR,
    CASE WHEN osbstd.UCORTK_categorie_mouvement = 2 THEN ooline.OBLNAM_montant_ligne_devise_commande ELSE 0 END AS VTE_CA_HT_BRUT_AVOIR,
    CASE WHEN osbstd.UCPIDE_code_promotion IS NOT NULL AND osbstd.UCPIDE_code_promotion != '' THEN osbstd.UCSAAM_montant_net_ligne ELSE 0 END AS VTE_CA_HT_NET_PROMO,
    CASE WHEN osbstd.UCPIDE_code_promotion IS NOT NULL AND osbstd.UCPIDE_code_promotion != '' THEN ooline.OBLNAM_montant_ligne_devise_commande ELSE 0 END AS VTE_CA_HT_BRUT_PROMO,
    CASE WHEN osbstd.UCAGNO_numero_contrat_chantier IS NOT NULL AND osbstd.UCAGNO_numero_contrat_chantier != '' THEN osbstd.UCSAAM_montant_net_ligne ELSE 0 END AS VTE_CA_HT_NET_AFFAIRE,
    CASE WHEN osbstd.UCAGNO_numero_contrat_chantier IS NOT NULL AND osbstd.UCAGNO_numero_contrat_chantier != '' THEN ooline.OBLNAM_montant_ligne_devise_commande ELSE 0 END AS VTE_CA_HT_BRUT_AFFAIRE,

    -- ========================================================================
    -- MESURES - REMISES (Pourcentages depuis OOLINE)
    -- ========================================================================

    ooline.OBDIP1_pct_remise_1 AS VTE_PCT_REMISE_1_STD,
    ooline.OBDIP2_pct_remise_2 AS VTE_PCT_REMISE_2_PROMO,
    ooline.OBDIP3_pct_remise_3 AS VTE_PCT_REMISE_3_VALISETTE,
    ooline.OBDIP4_pct_remise_4 AS VTE_PCT_REMISE_4_EXCEP,
    ooline.OBDIP5_pct_remise_5 AS VTE_PCT_REMISE_5_CHANTIER,
    ooline.OBDIP6_pct_remise_6 AS VTE_PCT_REMISE_6_PIED_CDV,

    -- ========================================================================
    -- MESURES - REMISES (Montants depuis OSBSTD)
    -- ========================================================================

    osbstd.UCDIA1_montant_remise_1 AS VTE_MTT_REMISE_1_STD,
    osbstd.UCDIA2_montant_remise_2 AS VTE_MTT_REMISE_2_PROMO,
    osbstd.UCDIA3_montant_remise_3 AS VTE_MTT_REMISE_3_VALISETTE,
    osbstd.UCDIA4_montant_remise_4 AS VTE_MTT_REMISE_4_EXCEP,
    osbstd.UCDIA5_montant_remise_5 AS VTE_MTT_REMISE_5_CHANTIER,
    osbstd.UCDIA6_montant_remise_6 AS VTE_MTT_REMISE_6_PIED_CDV,
    osbstd.UCOFRA_total_montant_remise AS VTE_MTT_REMISE_TOTAL,
    osbstd.UCDISY_modele_remise AS VTE_MODELE_REMISE,

    -- ========================================================================
    -- MESURES - COUTS ET MARGE
    -- ========================================================================

    osbstd.UCUCOS_cout_revient_saisie AS VTE_COUT_REVIENT_SAISIE,
    osbstd.UCDCOS_cout_revient_livraison AS VTE_COUT_REVIENT_LIVRAISON,

    -- Marge calculée
    (osbstd.UCSAAM_montant_net_ligne - osbstd.UCDCOS_cout_revient_livraison) AS VTE_MARGE_BRUTE,

    -- ========================================================================
    -- MESURES - POIDS ET VOLUME
    -- ========================================================================

    osbstd.UCGRWE_poids_brut AS VTE_POIDS_BRUT,
    osbstd.UCNEWE_poids_net AS VTE_POIDS_NET,
    osbstd.UCVOL3_volume AS VTE_VOLUME,

    -- ========================================================================
    -- INDICATEURS QUALITE
    -- ========================================================================

    osbstd.UCFULL_ligne_quantite_delai_ok AS VTE_IND_QTE_DELAI_OK,
    osbstd.UCRQTY_ligne_quantite_ok_delai_ko AS VTE_IND_QTE_OK_DELAI_KO,
    osbstd.UCRTME_ligne_delai_ok_quantite_ko AS VTE_IND_DELAI_OK_QTE_KO,
    osbstd.UCDLNI_livre_non_facture AS VTE_IND_LIVRE_NON_FACTURE,

    -- Ecarts de dates
    osbstd.UCDDF1_ecart_jours_depart_planifie_effectif AS VTE_ECART_JOURS_DEPART,
    osbstd.UCDDF2_ecart_jours_livraison_demandee_effective AS VTE_ECART_JOURS_LIV_DMD_EFF,
    osbstd.UCDDF3_ecart_jours_livraison_demandee_confirmee AS VTE_ECART_JOURS_LIV_DMD_CONF,
    osbstd.UCDDF4_ecart_jours_livraison_confirmee_effective AS VTE_ECART_JOURS_LIV_CONF_EFF,
    osbstd.UCDDF5_ecart_jours_livraison_facturation AS VTE_ECART_JOURS_LIV_FACT,

    -- Compteurs lignes
    osbstd.UCTORL_nombre_lignes_commande AS VTE_NB_LIGNES_COMMANDE,
    osbstd.UCTDEL_nombre_lignes_livrees AS VTE_NB_LIGNES_LIVREES,
    osbstd.UCLOWM_nombre_lignes_marge_basse AS VTE_NB_LIGNES_MARGE_BASSE,
    osbstd.UCMPRO_nombre_lignes_prix_manuel AS VTE_NB_LIGNES_PRIX_MANUEL,

    -- ========================================================================
    -- INFORMATIONS COMPLEMENTAIRES
    -- ========================================================================

    osbstd.UCORTP_type_commande_vente AS VTE_TYPE_COMMANDE,
    osbstd.UCORTK_categorie_mouvement AS VTE_CATEGORIE_MOUVEMENT,
    osbstd.UCLTYP_type_ligne AS VTE_TYPE_LIGNE,
    osbstd.UCPRMO_origine_prix AS VTE_ORIGINE_PRIX,
    osbstd.UCPRRF_code_tarif AS VTE_CODE_TARIF,
    osbstd.UCCUCD_devise AS VTE_DEVISE,
    osbstd.UCRAIN_taux_devise AS VTE_TAUX_DEVISE,
    osbstd.UCTEPY_condition_paiement AS VTE_CONDITION_PAIEMENT,
    osbstd.UCWCON_canal_entree_commande AS VTE_CANAL_COMMANDE,
    osbstd.UCRSCD_code_motif AS VTE_CODE_MOTIF,

    -- Audit
    osbstd.UCRGDT_date_creation AS VTE_DATE_CREATION,
    osbstd.UCLMDT_date_modification AS VTE_DATE_MODIFICATION,
    osbstd.UCLMTS_timestamp AS VTE_TIMESTAMP

FROM osbstd

LEFT JOIN ooline
    ON osbstd.UCCONO_code_societe = ooline.OBCONO_code_societe
    AND osbstd.UCORNO_numero_commande_client = ooline.OBORNO_numero_commande_client
    AND osbstd.UCPONR_numero_ligne_commande = ooline.OBPONR_numero_ligne_commande
    AND osbstd.UCPOSX_suffixe_ligne = ooline.OBPOSX_suffixe_ligne

LEFT JOIN ocusma
    ON osbstd.UCCONO_code_societe = ocusma.OKCONO_code_societe
    AND osbstd.UCCUNO_code_client_commande = ocusma.OKCUNO_code_client

WHERE osbstd.UCCONO_code_societe = 200
