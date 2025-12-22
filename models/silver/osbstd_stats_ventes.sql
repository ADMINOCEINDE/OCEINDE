{{ config(materialized='table', schema='SILVER') }}

SELECT
    UCDIVI AS UCDIVI_code_societe,
    RTRIM(UCSMCD) AS UCSMCD_code_representant,
    RTRIM(UCWHLO) AS UCWHLO_code_depot,
    RTRIM(UCCSCD) AS UCCSCD_code_pays,
    RTRIM(UCITNO) AS UCITNO_code_article,
    RTRIM(UCCUNO) AS UCCUNO_code_client,
    RTRIM(UCSDST) AS UCSDST_code_secteur,
    UCORDT AS UCORDT_date_planning,
    UCIVQT AS UCIVQT_quantite_nette,
    UCSAAM AS UCSAAM_ca_ht_net,
    UCIVNO AS UCIVNO_numero_facture,
    RTRIM(UCDLIX) AS UCDLIX_numero_bl,
    UCDIA1 AS UCDIA1_remise_1,
    UCDIA2 AS UCDIA2_remise_2,
    UCDIA3 AS UCDIA3_remise_3,
    UCDIA4 AS UCDIA4_remise_4,
    UCDIA5 AS UCDIA5_remise_5,
    UCDIA6 AS UCDIA6_remise_6,
    RTRIM(UCPIDE) AS UCPIDE_pied,
    RTRIM(UCAGNO) AS UCAGNO_numero_accord
FROM {{ source('db_test', 'OSBSTD') }}