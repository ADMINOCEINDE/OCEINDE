{{ config(materialized='table', schema='SILVER') }}

-- ============================================================================
-- OINVOL - Lignes de facturation
-- ============================================================================

SELECT
    -- Identifiants
    -- OINVOL AS ONCONO_code_societe,
    RTRIM(ONDIVI) AS ONDIVI_code_division,
    ONIVNO AS ONIVNO_numero_facture,
    ONYEA4 AS ONYEA4_annee_facture,
    
    -- Client
    RTRIM(ONPYNO) AS ONPYNO_code_client_payeur,
    
    -- Type d'information
    RTRIM(ONIVTP) AS ONIVTP_type_information,
    
    -- Références commande et livraison
    RTRIM(ONORNO) AS ONORNO_numero_commande,
    ONDLIX AS ONDLIX_numero_bl_m3,
    
    -- Dépôt
    RTRIM(ONWHLO) AS ONWHLO_code_depot_facturation,
    
    -- Références facture
    RTRIM(ONIVRF) AS ONIVRF_reference_facture,
    RTRIM(ONINPX) AS ONINPX_prefixe_facture,
    RTRIM(ONEXIN) AS ONEXIN_numero_facture_etendu,
    
    -- Montants
    ONIVAM AS ONIVAM_montant_facture_ht,
    ONIVLA AS ONIVLA_montant_facture_local,
    ONIVBA AS ONIVBA_montant_base_calcul_tva,
    ONIVAV AS ONIVAV_montant_facture_ttc,
    
    -- TVA
    ONCVT1 AS ONCVT1_montant_tva_1,
    ONCVT2 AS ONCVT2_montant_tva_2,
    ONVTAM AS ONVTAM_montant_tva_ligne,
    RTRIM(ONVTCD) AS ONVTCD_code_tva,
    ONVTP1 AS ONVTP1_taux_tva_1,
    ONVTP2 AS ONVTP2_taux_tva_2,
    
    -- Informations géographiques
    RTRIM(ONBSCD) AS ONBSCD_code_pays,
    RTRIM(ONECAR) AS ONECAR_code_region,
    
    -- TVA intracommunautaire
    RTRIM(ONVRIN) AS ONVRIN_numero_tva_intracom,
    RTRIM(ONVRNO) AS ONVRNO_numero_tva_depot,
    
    -- Condition de paiement
    RTRIM(ONTEPY) AS ONTEPY_condition_paiement,
    
    -- Dates
    ONRGDT AS ONRGDT_date_facture,
    ONRGTM AS ONRGTM_heure_facture,
    ONLMDT AS ONLMDT_date_modification,
    ONCHNO AS ONCHNO_nombre_modifications,
    RTRIM(ONCHID) AS ONCHID_utilisateur_modification

FROM {{ source('db_test', 'OINVOL') }}