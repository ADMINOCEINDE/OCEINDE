{{ config(materialized='table', schema='SILVER') }}

-- ============================================================================
-- ODLINE - Lignes de livraison
-- ============================================================================

SELECT
    -- Identifiants
    UBCONO AS UBCONO_code_societe,
    RTRIM(UBDIVI) AS UBDIVI_code_division,
    RTRIM(UBORNO) AS UBORNO_numero_commande_client,
    UBPONR AS UBPONR_numero_ligne_commande,
    UBPOSX AS UBPOSX_suffixe_ligne,
    UBDLIX AS UBDLIX_numero_livraison,
    
    -- Entités logistiques
    RTRIM(UBFACI) AS UBFACI_code_enseigne,
    RTRIM(UBWHLO) AS UBWHLO_code_depot,
    RTRIM(UBDECU) AS UBDECU_code_client_livre,
    
    -- Article
    RTRIM(UBITNO) AS UBITNO_code_article,
    RTRIM(UBLTYP) AS UBLTYP_type_ligne,
    
    -- Quantités livrées
    UBDLQT AS UBDLQT_quantite_livree_um,
    UBDLQA AS UBDLQA_quantite_livree_um_alt,
    UBDLQS AS UBDLQS_quantite_livree_um_prix,
    
    -- Quantités facturées
    UBIVQT AS UBIVQT_quantite_facturee_um,
    UBIVQA AS UBIVQA_quantite_facturee_um_alt,
    UBIVQS AS UBIVQS_quantite_facturee_um_prix,
    
    -- Quantités retournées
    UBRTQT AS UBRTQT_quantite_retournee_um,
    UBRTQA AS UBRTQA_quantite_retournee_um_alt,
    
    -- Ecart quantité
    UBCHQT AS UBCHQT_ecart_quantite,
    
    -- Prix
    UBSAPR AS UBSAPR_prix_brut_unitaire,
    UBNEPR AS UBNEPR_prix_net_unitaire,
    UBSACD AS UBSACD_par_combien_pcb,
    RTRIM(UBPRMO) AS UBPRMO_origine_prix,
    
    -- Remises (statuts)
    RTRIM(UBDIC1) AS UBDIC1_origine_remise_1,
    RTRIM(UBDIC2) AS UBDIC2_origine_remise_2,
    RTRIM(UBDIC3) AS UBDIC3_origine_remise_3,
    RTRIM(UBDIC4) AS UBDIC4_origine_remise_4,
    RTRIM(UBDIC5) AS UBDIC5_origine_remise_5,
    RTRIM(UBDIC6) AS UBDIC6_origine_remise_6,
    
    -- Remises (pourcentages)
    UBDIP1 AS UBDIP1_pct_remise_1,
    UBDIP2 AS UBDIP2_pct_remise_2,
    UBDIP3 AS UBDIP3_pct_remise_3,
    UBDIP4 AS UBDIP4_pct_remise_4,
    UBDIP5 AS UBDIP5_pct_remise_5,
    UBDIP6 AS UBDIP6_pct_remise_6,
    
    -- Remises (montants)
    UBDIA1 AS UBDIA1_montant_remise_1,
    UBDIA2 AS UBDIA2_montant_remise_2,
    UBDIA3 AS UBDIA3_montant_remise_3,
    UBDIA4 AS UBDIA4_montant_remise_4,
    UBDIA5 AS UBDIA5_montant_remise_5,
    UBDIA6 AS UBDIA6_montant_remise_6,
    
    -- Montant ligne
    UBLNAM AS UBLNAM_montant_ligne_devise_commande,
    
    -- Coûts
    UBUCOS AS UBUCOS_cout_standard_unitaire,
    UBUCCD AS UBUCCD_methode_valorisation,
    UBDCOS AS UBDCOS_montant_cout_ligne,
    RTRIM(UBINPR) AS UBINPR_prix_interco,
    RTRIM(UBCUCT) AS UBCUCT_devise_interco,
    
    -- Unités
    RTRIM(UBALUN) AS UBALUN_unite_alternative,
    RTRIM(UBSPUN) AS UBSPUN_unite_prix_vente,
    
    -- Gestion stock
    UBSTCD AS UBSTCD_article_gere_stock,
    
    -- Campagne et promotion
    RTRIM(UBCMNO) AS UBCMNO_campagne_prix_vente,
    RTRIM(UBPIDE) AS UBPIDE_code_promotion,
    
    -- Facture
    UBIVNO AS UBIVNO_numero_facture,
    UBYEA4 AS UBYEA4_annee_facture,
    RTRIM(UBINPX) AS UBINPX_prefixe_facture,
    RTRIM(UBEXIN) AS UBEXIN_numero_facture_etendu,
    
    -- Référence commande client
    RTRIM(UBCUOR) AS UBCUOR_reference_commande_client,
    
    -- Condition de paiement
    RTRIM(UBTEPY) AS UBTEPY_condition_paiement,
    
    -- Classification ligne
    UBLNCL AS UBLNCL_classification_ligne,
    
    -- Dates
    UBRGDT AS UBRGDT_date_livraison,
    UBRGTM AS UBRGTM_heure_livraison,
    UBLMDT AS UBLMDT_date_modification,
    UBCHNO AS UBCHNO_nombre_modifications,
    RTRIM(UBCHID) AS UBCHID_utilisateur_modification

FROM {{ source('db_test', 'ODLINE') }}