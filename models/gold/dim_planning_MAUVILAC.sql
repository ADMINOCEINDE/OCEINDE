{{ config(materialized='table', schema='GOLD') }}

SELECT
    CDDIVI_code_societe AS code_societe,
    CDYMD8_date_planning AS date_planning,
    CDYMD8_annee AS annee,
    CDYMD8_mois AS mois,
    CDYMD8_jour AS jour,
    CDBDAY_type_jour_code AS type_jour_code,
    CDYWD5_semaine_annee AS semaine_annee,
    CDYWD5_semaine_num AS semaine_numero
FROM {{ ref('csycal_calendrier') }}
WHERE CDDIVI_code_societe = '200'