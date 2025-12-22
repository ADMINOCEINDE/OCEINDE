{{ config(materialized='table', schema='SILVER') }}

SELECT
    OKCONO AS OKCONO_code_societe,
    RTRIM(OKCUNO) AS OKCUNO_code_client,
    RTRIM(OKSMCD) AS OKSMCD_code_representant
FROM {{ source('db_test', 'CCUDIV') }}