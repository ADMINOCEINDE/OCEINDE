{{ config(materialized='table', schema='SILVER') }}

-- ============================================================================
-- OSBSTD - Statistiques de vente
-- ============================================================================

SELECT
    -- Identifiants
    UCCONO AS UCCONO_code_societe,
    RTRIM(UCDIVI) AS UCDIVI_code_division,
    RTRIM(UCORNO) AS UCORNO_numero_commande_client,
    UCPONR AS UCPONR_numero_ligne_commande,
    UCPOSX AS UCPOSX_suffixe_ligne,
    
    -- Références
    UCDLIX AS UCDLIX_numero_bl_m3,
    UCIVNO AS UCIVNO_numero_facture,
    UCYEA4 AS UCYEA4_annee_facturation,
    RTRIM(UCEXIN) AS UCEXIN_numero_facture_etendu,
    
    -- Entités logistiques
    RTRIM(UCFACI) AS UCFACI_code_etablissement,
    RTRIM(UCWHLO) AS UCWHLO_code_depot,
    
    -- Article
    RTRIM(UCITNO) AS UCITNO_code_article,
    RTRIM(UCREPI) AS UCREPI_code_article_remplace,
    RTRIM(UCITCL) AS UCITCL_groupe_produit,
    RTRIM(UCITGR) AS UCITGR_groupe_article,
    RTRIM(UCITTY) AS UCITTY_type_article,
    RTRIM(UCBUAR) AS UCBUAR_secteur_activite,
    
    -- Client
    RTRIM(UCCUNO) AS UCCUNO_code_client_commande,
    RTRIM(UCDECU) AS UCDECU_code_client_livre,
    RTRIM(UCINRC) AS UCINRC_code_client_facture,
    RTRIM(UCPYNO) AS UCPYNO_code_client_payeur,
    RTRIM(UCCUST) AS UCCUST_client_statistique,
    RTRIM(UCCUCL) AS UCCUCL_groupe_client,
    RTRIM(UCCUTP) AS UCCUTP_type_client,
    
    -- Type commande
    RTRIM(UCORTP) AS UCORTP_type_commande_vente,
    UCORTK AS UCORTK_categorie_mouvement,
    RTRIM(UCLTYP) AS UCLTYP_type_ligne,
    
    -- Quantités commandées
    UCORQT AS UCORQT_quantite_commandee_um,
    UCORQA AS UCORQA_quantite_commandee_um_alt,
    UCORQS AS UCORQS_quantite_commandee_um_prix,
    UCORQB AS UCORQB_quantite_commandee_um_stat,
    UCDEMA AS UCDEMA_quantite_demande_initiale,
    UCDMA2 AS UCDMA2_gestion_demande,
    
    -- Quantités facturées
    UCIVQT AS UCIVQT_quantite_facturee_um,
    UCIVQA AS UCIVQA_quantite_facturee_um_alt,
    UCIVQS AS UCIVQS_quantite_facturee_um_prix,
    UCOFQS AS UCOFQS_quantite_facturee_um_stat,
    
    -- Unités
    RTRIM(UCSPUN) AS UCSPUN_unite_prix_vente,
    RTRIM(UCSTUN) AS UCSTUN_unite_statistique,
    
    -- Prix et coûts
    RTRIM(UCPRMO) AS UCPRMO_origine_prix,
    RTRIM(UCPRRF) AS UCPRRF_code_tarif,
    UCUCOS AS UCUCOS_cout_revient_saisie,
    UCDCOS AS UCDCOS_cout_revient_livraison,
   -- UCUCUCD AS UCUCUCD_code_methode_cout,
    
    -- Montants
    UCSAAM AS UCSAAM_montant_net_ligne,
    UCCUAM AS UCCUAM_montant_net_devise_tiers,
    UCSGAM AS UCSGAM_montant_prix_vente_fiche,
    RTRIM(UCCUCD) AS UCCUCD_devise,
    UCRAIN AS UCRAIN_taux_devise,
    
    -- Remises
    UCDIA1 AS UCDIA1_montant_remise_1,
    UCDIA2 AS UCDIA2_montant_remise_2,
    UCDIA3 AS UCDIA3_montant_remise_3,
    UCDIA4 AS UCDIA4_montant_remise_4,
    UCDIA5 AS UCDIA5_montant_remise_5,
    UCDIA6 AS UCDIA6_montant_remise_6,
    UCOFRA AS UCOFRA_total_montant_remise,
    RTRIM(UCDISY) AS UCDISY_modele_remise,
    
    -- Campagnes et promotions
    RTRIM(UCCMNO) AS UCCMNO_campagne_tarifaire,
    RTRIM(UCCMP1) AS UCCMP1_campagne_remise_1,
    RTRIM(UCCMP2) AS UCCMP2_campagne_remise_2,
    RTRIM(UCCMP3) AS UCCMP3_campagne_valisette,
    RTRIM(UCCMP4) AS UCCMP4_campagne_remise_4,
    RTRIM(UCCMP5) AS UCCMP5_campagne_remise_5,
    RTRIM(UCCMP6) AS UCCMP6_campagne_remise_6,
    RTRIM(UCPIDE) AS UCPIDE_code_promotion,
    
    -- Dates
    UCORDT AS UCORDT_date_commande,
    UCDWDT AS UCDWDT_date_livraison_demandee,
    UCCODT AS UCCODT_date_livraison_confirmee,
    UCDLDT AS UCDLDT_date_livraison_planifiee,
    UCIVDT AS UCIVDT_date_facture,
    UCACDT AS UCACDT_date_comptable,
    
    -- Ecarts de dates (jours)
    UCDDF1 AS UCDDF1_ecart_jours_depart_planifie_effectif,
    UCDDF2 AS UCDDF2_ecart_jours_livraison_demandee_effective,
    UCDDF3 AS UCDDF3_ecart_jours_livraison_demandee_confirmee,
    UCDDF4 AS UCDDF4_ecart_jours_livraison_confirmee_effective,
    UCDDF5 AS UCDDF5_ecart_jours_livraison_facturation,
    
    -- Indicateurs qualité
    UCFULL AS UCFULL_ligne_quantite_delai_ok,
    UCRQTY AS UCRQTY_ligne_quantite_ok_delai_ko,
    UCRTME AS UCRTME_ligne_delai_ok_quantite_ko,
    UCDLNI AS UCDLNI_livre_non_facture,
    
    -- Compteurs lignes
    UCTORL AS UCTORL_nombre_lignes_commande,
    UCTDEL AS UCTDEL_nombre_lignes_livrees,
    UCLOWM AS UCLOWM_nombre_lignes_marge_basse,
    UCMPRO AS UCMPRO_nombre_lignes_prix_manuel,
    
    -- RFA et commissions
    UCBAOL AS UCBAOL_nombre_lignes_calcul_rfa,
    UCBGOL AS UCBGOL_nombre_lignes_paiement_rfa,
    UCCAOL AS UCCAOL_nombre_lignes_calcul_commission,
    UCCGOL AS UCCGOL_nombre_lignes_paiement_commission,
    UCALBO AS UCALBO_montant_rfa_provision,
    UCSMCC AS UCSMCC_controle_marge,
    
    -- Commercial
    RTRIM(UCSMCD) AS UCSMCD_code_representant,
    RTRIM(UCSDEP) AS UCSDEP_departement_vente,
    RTRIM(UCSDST) AS UCSDST_region,
    
    -- Logistique
    RTRIM(UCROUT) AS UCROUT_tournee,
    RTRIM(UCRODN) AS UCRODN_depart_tournee,
    RTRIM(UCADID) AS UCADID_adresse_livraison,
    
    -- Contrat
    RTRIM(UCAGNO) AS UCAGNO_numero_contrat_chantier,
    RTRIM(UCAGNT) AS UCAGNT_beneficiaire_client,
    
    -- Paiement
    RTRIM(UCTEPY) AS UCTEPY_condition_paiement,
    
    -- Poids et volume
    UCGRWE AS UCGRWE_poids_brut,
    UCNEWE AS UCNEWE_poids_net,
    UCVOL3 AS UCVOL3_volume,
    
    -- Configuration et attributs
    RTRIM(UCCFIN) AS UCCFIN_numero_configuration,
    RTRIM(UCATMO) AS UCATMO_modele_attribut,
    UCATV1 AS UCATV1_valeur_attribut_1,
    UCATV2 AS UCATV2_valeur_attribut_2,
    UCATV3 AS UCATV3_valeur_attribut_3,
    UCATV4 AS UCATV4_valeur_attribut_4,
    UCATV5 AS UCATV5_valeur_attribut_5,
    UCAAV1 AS UCAAV1_valeur_attribut_calc_1,
    UCAAV2 AS UCAAV2_valeur_attribut_calc_2,
    UCAAV3 AS UCAAV3_valeur_attribut_calc_3,
    UCAAV4 AS UCAAV4_valeur_attribut_calc_4,
    UCAAV5 AS UCAAV5_valeur_attribut_calc_5,
    
    -- Variante
    RTRIM(UCVANO) AS UCVANO_variante_produit,
    
    -- Statistiques
    RTRIM(UCFRE1) AS UCFRE1_id_stat_client_1,
    RTRIM(UCFRE2) AS UCFRE2_id_stat_client_2,
    RTRIM(UCFRE3) AS UCFRE3_zone_stat_article_3,
    RTRIM(UCFRE4) AS UCFRE4_zone_stat_article_4,
    
    -- Circuit commercial
    RTRIM(UCCHL1) AS UCCHL1_circuit_commercial_1,
    RTRIM(UCCHL2) AS UCCHL2_circuit_commercial_2,
    RTRIM(UCCHL3) AS UCCHL3_circuit_commercial_3,
    RTRIM(UCCHL4) AS UCCHL4_circuit_commercial_4,
    
    -- Canal et motif
    RTRIM(UCWCON) AS UCWCON_canal_entree_commande,
    RTRIM(UCRSCD) AS UCRSCD_code_motif,
    RTRIM(UCRSC1) AS UCRSC1_code_motif_transaction,
    
    -- Géographie
    RTRIM(UCCSCD) AS UCCSCD_code_pays,
    
    -- Origine
    UCORIG AS UCORIG_origine_ordre,
    
    -- Multi-société
    UCMUFT AS UCMUFT_info_multi_societe,
    UCDMCU AS UCDMCU_methode_conversion_devise,
    
    -- Fournisseur
    RTRIM(UCSUNO) AS UCSUNO_fournisseur,
    
    -- Audit
    UCRGDT AS UCRGDT_date_creation,
    UCRGTM AS UCRGTM_heure_creation,
    UCLMDT AS UCLMDT_date_modification,
    UCCHNO AS UCCHNO_nombre_modifications,
    RTRIM(UCCHID) AS UCCHID_utilisateur_modification,
    UCLMTS AS UCLMTS_timestamp

FROM {{ source('db_test', 'OSBSTD') }}