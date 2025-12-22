{{ config(materialized='table', schema='SILVER') }}

SELECT
*
--    UBCONO AS UBCONO_code_societe,  
--    RTRIM(UBDIVI) AS UBDIVI_code_division,
--    RTRIM(UBIVNO) AS UBIVNO_numero_facture,
--    RTRIM(UBDLIX) AS UBDLIX_numero_bl,
  --  RTRIM(UBCUNO) AS UBCUNO_code_client,
  --  UBIVDT AS UBIVDT_date_facture
FROM {{ source('db_test', 'OINVOL') }}