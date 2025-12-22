{{ config(materialized='table', schema='SILVER') }}

SELECT
    RTRIM(CDDIVI) AS CDDIVI_code_societe,
    CDYMD8 AS CDYMD8_date_planning,
    SUBSTRING(CAST(CDYMD8 AS CHAR(8)), 1, 4) AS CDYMD8_annee,
    SUBSTRING(CAST(CDYMD8 AS CHAR(8)), 5, 2) AS CDYMD8_mois,
    SUBSTRING(CAST(CDYMD8 AS CHAR(8)), 7, 2) AS CDYMD8_jour,
    RTRIM(CDBDAY) AS CDBDAY_type_jour_code,
    SUBSTRING(CAST(CDYWD5 AS CHAR(5)), 1, 4) AS CDYWD5_semaine_annee,
    SUBSTRING(CAST(CDYWD5 AS CHAR(5)), 3, 2) AS CDYWD5_semaine_num
FROM {{ source('db_test', 'CSYCAL') }}