{{ config(materialized='table', schema='SILVER') }}

SELECT
    OKCONO AS OKCONO_code_societe,
    RTRIM(OKCUNO) AS OKCUNO_code_client,
    RTRIM(OKCUNM) AS OKCUNM_nom_client,
    RTRIM(OKSTAT) AS OKSTAT_statut,
    RTRIM(OKBGRP) AS OKBGRP_groupe_budget,
    RTRIM(OKCFC4) AS OKCFC4_champ_libre_4,
    RTRIM(OKCFC8) AS OKCFC8_groupement_code,  -- Nouvelle colonne
    RTRIM(OKCFC3) AS OKCFC3_groupe_client_code,  -- Renommée pour plus de clarté
    RTRIM(OKECAR) AS OKECAR_code_departement  -- Nouvelle colonne
FROM {{ source('db_test', 'OCUSMA') }}