{{ config(materialized='table', schema='GOLD') }}

WITH clients AS (
    SELECT *
    FROM {{ ref('ocusma_clients') }}
),

clients_divisions AS (
    SELECT *
    FROM {{ ref('ccudiv_clients_divisions') }}
),

parametres AS (
    SELECT *
    FROM {{ ref('csytab_parametres_systeme') }}
)

SELECT
    a.OKCONO_code_societe AS code_societe,
    a.OKCUNO_code_client AS code_client,
    a.OKCUNM_nom_client AS nom_client,
    a.OKSTAT_statut AS statut_client,
    b.OKSMCD_code_representant AS code_representant,
    c.CTTX40_libelle AS libelle_representant,
    a.OKBGRP_groupe_budget AS groupe_budget,
    a.OKCFC4_champ_libre_4 AS champ_libre_4,
    a.OKCFC8_groupement_code AS VTE_GROUPEMENT_CODE,
    a.OKCFC3_groupe_client_code AS VTE_GROUPECLI_CODE

FROM clients a

LEFT JOIN clients_divisions b
    ON a.OKCONO_code_societe = b.OKCONO_code_societe
   AND a.OKCUNO_code_client = b.OKCUNO_code_client

LEFT JOIN parametres c
    ON b.OKSMCD_code_representant = c.CTSTKY_cle
   AND c.CTCONO_code_societe = b.OKCONO_code_societe
   AND c.CTSTCO_type_table = 'SMCD'

WHERE a.OKCONO_code_societe = '200'