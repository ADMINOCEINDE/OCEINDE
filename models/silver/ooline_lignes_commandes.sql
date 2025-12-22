{{ config(materialized='table', schema='SILVER') }}

-- ============================================================================
-- OOLINE - Lignes de commandes de vente
-- ============================================================================

SELECT
    -- Identifiants
    OBCONO AS OBCONO_code_societe,
    RTRIM(OBDIVI) AS OBDIVI_code_division,
    RTRIM(OBORNO) AS OBORNO_numero_commande_client,
    OBPONR AS OBPONR_numero_ligne_commande,
    OBPOSX AS OBPOSX_suffixe_ligne,
    
    -- Informations article
    RTRIM(OBITNO) AS OBITNO_code_article,
    RTRIM(OBREPI) AS OBREPI_code_article_remplace,
    RTRIM(OBITDS) AS OBITDS_libelle_article,
    RTRIM(OBTEDS) AS OBTEDS_description_article,
    
    -- Statuts et types
    RTRIM(OBLTYP) AS OBLTYP_type_ligne,
    RTRIM(OBORST) AS OBORST_statut_ligne,
    
    -- Entités logistiques
    RTRIM(OBFACI) AS OBFACI_code_enseigne,
    RTRIM(OBWHLO) AS OBWHLO_code_depot,
    RTRIM(OBCUNO) AS OBCUNO_code_client,
    RTRIM(OBDECU) AS OBDECU_code_client_livre,
    RTRIM(OBADID) AS OBADID_code_adresse_livraison,
    
    -- Quantités commandées
    OBORQT AS OBORQT_quantite_commandee_um,
    OBORQA AS OBORQA_quantite_commandee_um_alt,
    
    -- Quantités restantes
    OBRNQT AS OBRNQT_quantite_restante_um,
    OBRNQA AS OBRNQA_quantite_restante_um_alt,
    
    -- Quantités allouées
    OBALQT AS OBALQT_quantite_allouee_um,
    OBALQA AS OBALQA_quantite_allouee_um_alt,
    
    -- Quantités en préparation
    OBPLQT AS OBPLQT_quantite_en_preparation_um,
    OBPLQA AS OBPLQA_quantite_en_preparation_um_alt,
    
    -- Quantités livrées
    OBDLQT AS OBDLQT_quantite_livree_um,
    OBDLQA AS OBDLQA_quantite_livree_um_alt,
    
    -- Quantités facturées
    OBIVQT AS OBIVQT_quantite_facturee_um,
    OBIVQA AS OBIVQA_quantite_facturee_um_alt,
    
    -- Unités de mesure
    RTRIM(OBALUN) AS OBALUN_unite_alternative,
    RTRIM(OBSPUN) AS OBSPUN_unite_tarifaire,
    OBCOFA AS OBCOFA_facteur_conversion,
    OBDMCF AS OBDMCF_sens_conversion,
    
    -- Prix et tarification
    OBSAPR AS OBSAPR_prix_brut,
    OBNEPR AS OBNEPR_prix_net,
    OBSACD AS OBSACD_par_combien_pcb,
    RTRIM(OBPRMO) AS OBPRMO_origine_prix,
    RTRIM(OBPRRF) AS OBPRRF_code_tarif,
    RTRIM(OBCUCD) AS OBCUCD_devise,
    
    -- Remises (statuts)
    RTRIM(OBDIC1) AS OBDIC1_origine_remise_1,
    RTRIM(OBDIC2) AS OBDIC2_origine_remise_2,
    RTRIM(OBDIC3) AS OBDIC3_origine_remise_3,
    RTRIM(OBDIC4) AS OBDIC4_origine_remise_4,
    RTRIM(OBDIC5) AS OBDIC5_origine_remise_5,
    RTRIM(OBDIC6) AS OBDIC6_origine_remise_6,
    
    -- Remises (pourcentages)
    OBDIP1 AS OBDIP1_pct_remise_1,
    OBDIP2 AS OBDIP2_pct_remise_2,
    OBDIP3 AS OBDIP3_pct_remise_3,
    OBDIP4 AS OBDIP4_pct_remise_4,
    OBDIP5 AS OBDIP5_pct_remise_5,
    OBDIP6 AS OBDIP6_pct_remise_6,
    
    -- Remises (montants)
    OBDIA1 AS OBDIA1_montant_remise_1,
    OBDIA2 AS OBDIA2_montant_remise_2,
    OBDIA3 AS OBDIA3_montant_remise_3,
    OBDIA4 AS OBDIA4_montant_remise_4,
    OBDIA5 AS OBDIA5_montant_remise_5,
    OBDIA6 AS OBDIA6_montant_remise_6,
    
    -- Campagnes remises
    RTRIM(OBCMP1) AS OBCMP1_code_campagne_remise_1,
    RTRIM(OBCMP2) AS OBCMP2_code_campagne_remise_2,
    RTRIM(OBCMP3) AS OBCMP3_code_campagne_remise_3,
    RTRIM(OBCMP4) AS OBCMP4_code_campagne_remise_4,
    RTRIM(OBCMP5) AS OBCMP5_code_campagne_remise_5,
    RTRIM(OBCMP6) AS OBCMP6_code_campagne_remise_6,
    
    -- Montants ligne
    OBLNAM AS OBLNAM_montant_ligne_devise_commande,
    OBLNA2 AS OBLNA2_montant_ligne_devise_societe,
    
    -- Coûts
    OBUCOS AS OBUCOS_cout_standard_unitaire,
    OBCOCD AS OBCOCD_pcb_valorisation,
    OBUCCD AS OBUCCD_methode_valorisation,
    RTRIM(OBINPR) AS OBINPR_prix_interco,
    RTRIM(OBCUCT) AS OBCUCT_devise_interco,
    
    -- Dates livraison demandées (fuseau société)
    OBDWDT AS OBDWDT_date_livraison_demandee,
    OBDWHM AS OBDWHM_heure_livraison_demandee,
    
    -- Dates livraison confirmées (fuseau société)
    OBCODT AS OBCODT_date_livraison_confirmee,
    OBCOHM AS OBCOHM_heure_livraison_confirmee,
    
    -- Dates livraison (fuseau client)
    OBDWDZ AS OBDWDZ_date_livraison_demandee_client,
    OBCODZ AS OBCODZ_date_livraison_confirmee_client,
    
    -- Planning
    OBPLDT AS OBPLDT_date_planifiee,
    OBPLHM AS OBPLHM_heure_planifiee,
    
    -- Tournée et départ
    RTRIM(OBROUT) AS OBROUT_code_tournee,
    RTRIM(OBRODN) AS OBRODN_code_depart_tournee,
    OBDSDT AS OBDSDT_date_depart,
    OBDSHM AS OBDSHM_heure_depart,
    
    -- Informations commerciales
    RTRIM(OBSMCD) AS OBSMCD_code_representant,
    RTRIM(OBCMNO) AS OBCMNO_campagne_prix_vente,
    RTRIM(OBPIDE) AS OBPIDE_code_promotion,
    RTRIM(OBAGNO) AS OBAGNO_numero_contrat_chantier,
    
    -- TVA et conditions
    RTRIM(OBVTCD) AS OBVTCD_code_tva,
    RTRIM(OBTEPY) AS OBTEPY_condition_paiement,
    OBPMOR AS OBPMOR_origine_condition_paiement,
    
    -- Configuration et attributs
    RTRIM(OBCFIN) AS OBCFIN_numero_configuration,
    RTRIM(OBATMO) AS OBATMO_modele_attribut,
    OBATNR AS OBATNR_numero_attribut,
    
    -- Livraison groupée
    RTRIM(OBJDCD) AS OBJDCD_code_livraison_groupee,
    RTRIM(OBDLSP) AS OBDLSP_specification_livraison,
    
    -- Conditionnement
    RTRIM(OBTEPA) AS OBTEPA_modalite_conditionnement,
    RTRIM(OBPACT) AS OBPACT_conditionnement,
    RTRIM(OBCUPA) AS OBCUPA_identifiant_conditionnement_client,
    OBD1QT AS OBD1QT_quantite_std_conditionnement,
    
    -- Mode livraison
    RTRIM(OBMODL) AS OBMODL_mode_livraison,
    RTRIM(OBTEDL) AS OBTEDL_condition_livraison,
    
    -- RFA et commissions
    OBBNCD AS OBBNCD_generation_rfa,
    OBPRAC AS OBPRAC_generation_commission,
    
    -- Commerce extérieur
    RTRIM(OBVRCD) AS OBVRCD_type_activite,
    RTRIM(OBECLC) AS OBECLC_code_statistique_commerce,
    RTRIM(OBORCO) AS OBORCO_pays_origine,
    
    -- Référence client
    RTRIM(OBCUOR) AS OBCUOR_numero_commande_client_ref,
    RTRIM(OBPOPN) AS OBPOPN_reference_complementaire,
    
    -- Motif et priorité
    RTRIM(OBRSCD) AS OBRSCD_code_motif,
    OBPRIO AS OBPRIO_priorite_ligne,
    OBLNCL AS OBLNCL_classification_ligne,
    
    -- Audit
    OBRGDT AS OBRGDT_date_creation,
    OBRGTM AS OBRGTM_heure_creation,
    OBLMDT AS OBLMDT_date_modification,
    OBCHNO AS OBCHNO_nombre_modifications,
    RTRIM(OBCHID) AS OBCHID_utilisateur_modification,
    OBLMTS AS OBLMTS_timestamp

FROM {{ source('db_test', 'OOLINE') }}