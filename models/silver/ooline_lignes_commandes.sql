{{ config(materialized='table', schema='SILVER') }}

SELECT
    OBCONO AS OACONO_code_societe
  --  RTRIM(OAORNO) AS OAORNO_numero_commande,
   -- OAPONR AS OAPONR_numero_ligne,
  --  RTRIM(OBITNO) AS OBITNO_code_article,
   --
   -- RTRIM(OBCUNO) AS OBCUNO_code_client,
  --  OBLNAM AS OBLNAM_ca_ht_brut,
   -- OBDIP1 AS OBDIP1_remise_1,
 --    OBDIP2 AS OBDIP2_remise_2,
  --  OBDIP3 AS OBDIP3_remise_3,
--    OBDIP4 AS OBDIP4_remise_4,
--    OBDIP5 AS OBDIP5_remise_5,
--    OBDIP6 AS OBDIP6_remise_6
FROM {{ source('db_test', 'OOLINE') }}