{{ config(materialized='table', schema='GOLD') }}

WITH date_range AS (
    SELECT DATE('2020-01-01') AS date_debut,
           CURRENT_DATE AS date_fin
),

calendar AS (
    SELECT
        CAST(date_debut + (ROW_NUMBER() OVER (ORDER BY 1) - 1) DAYS AS DATE) AS date_complete
    FROM date_range
    CROSS JOIN TABLE(VALUES 1,2,3,4,5,6,7,8,9,10) AS t1(n)
    CROSS JOIN TABLE(VALUES 1,2,3,4,5,6,7,8,9,10) AS t2(n)
    CROSS JOIN TABLE(VALUES 1,2,3,4,5,6,7,8,9,10) AS t3(n)
    CROSS JOIN TABLE(VALUES 1,2,3,4,5,6,7,8,9,10) AS t4(n)
    WHERE date_debut + (ROW_NUMBER() OVER (ORDER BY 1) - 1) DAYS <= (SELECT date_fin FROM date_range)
)

SELECT
    -- Clé primaire
    REPLACE(CAST(date_complete AS VARCHAR(10)), '-', '') AS CAL_DATE_ID,

    -- Date complète
    date_complete AS CAL_DATE,

    -- Année
    YEAR(date_complete) AS CAL_ANNEE,
    CAST(YEAR(date_complete) AS VARCHAR(4)) AS CAL_ANNEE_LIB,

    -- Trimestre
    QUARTER(date_complete) AS CAL_TRIMESTRE,
    'T' || CAST(QUARTER(date_complete) AS VARCHAR(1)) || ' ' || CAST(YEAR(date_complete) AS VARCHAR(4)) AS CAL_TRIMESTRE_LIB,
    CAST(YEAR(date_complete) AS VARCHAR(4)) || 'T' || CAST(QUARTER(date_complete) AS VARCHAR(1)) AS CAL_TRIMESTRE_CODE,

    -- Mois
    MONTH(date_complete) AS CAL_MOIS,
    CASE MONTH(date_complete)
        WHEN 1 THEN 'Janvier'
        WHEN 2 THEN 'Février'
        WHEN 3 THEN 'Mars'
        WHEN 4 THEN 'Avril'
        WHEN 5 THEN 'Mai'
        WHEN 6 THEN 'Juin'
        WHEN 7 THEN 'Juillet'
        WHEN 8 THEN 'Août'
        WHEN 9 THEN 'Septembre'
        WHEN 10 THEN 'Octobre'
        WHEN 11 THEN 'Novembre'
        WHEN 12 THEN 'Décembre'
    END AS CAL_MOIS_LIB,
    CASE MONTH(date_complete)
        WHEN 1 THEN 'Jan'
        WHEN 2 THEN 'Fév'
        WHEN 3 THEN 'Mar'
        WHEN 4 THEN 'Avr'
        WHEN 5 THEN 'Mai'
        WHEN 6 THEN 'Juin'
        WHEN 7 THEN 'Juil'
        WHEN 8 THEN 'Août'
        WHEN 9 THEN 'Sep'
        WHEN 10 THEN 'Oct'
        WHEN 11 THEN 'Nov'
        WHEN 12 THEN 'Déc'
    END AS CAL_MOIS_COURT,
    CAST(YEAR(date_complete) AS VARCHAR(4)) || RIGHT('0' || CAST(MONTH(date_complete) AS VARCHAR(2)), 2) AS CAL_MOIS_CODE,
    RIGHT('0' || CAST(MONTH(date_complete) AS VARCHAR(2)), 2) || '/' || CAST(YEAR(date_complete) AS VARCHAR(4)) AS CAL_MOIS_ANNEE_LIB,

    -- Semaine
    WEEK(date_complete) AS CAL_SEMAINE,
    'S' || RIGHT('0' || CAST(WEEK(date_complete) AS VARCHAR(2)), 2) || ' ' || CAST(YEAR(date_complete) AS VARCHAR(4)) AS CAL_SEMAINE_LIB,
    CAST(YEAR(date_complete) AS VARCHAR(4)) || 'S' || RIGHT('0' || CAST(WEEK(date_complete) AS VARCHAR(2)), 2) AS CAL_SEMAINE_CODE,

    -- Jour
    DAY(date_complete) AS CAL_JOUR,
    DAYOFYEAR(date_complete) AS CAL_JOUR_ANNEE,
    DAYOFWEEK(date_complete) AS CAL_JOUR_SEMAINE_NUM,
    CASE DAYOFWEEK(date_complete)
        WHEN 1 THEN 'Dimanche'
        WHEN 2 THEN 'Lundi'
        WHEN 3 THEN 'Mardi'
        WHEN 4 THEN 'Mercredi'
        WHEN 5 THEN 'Jeudi'
        WHEN 6 THEN 'Vendredi'
        WHEN 7 THEN 'Samedi'
    END AS CAL_JOUR_SEMAINE_LIB,
    CASE DAYOFWEEK(date_complete)
        WHEN 1 THEN 'Dim'
        WHEN 2 THEN 'Lun'
        WHEN 3 THEN 'Mar'
        WHEN 4 THEN 'Mer'
        WHEN 5 THEN 'Jeu'
        WHEN 6 THEN 'Ven'
        WHEN 7 THEN 'Sam'
    END AS CAL_JOUR_SEMAINE_COURT,

    -- Indicateurs booléens
    CASE WHEN DAYOFWEEK(date_complete) IN (1, 7) THEN 'Oui' ELSE 'Non' END AS CAL_EST_WEEKEND,
    CASE WHEN DAYOFWEEK(date_complete) NOT IN (1, 7) THEN 'Oui' ELSE 'Non' END AS CAL_EST_JOUR_SEMAINE,
    CASE WHEN date_complete = CURRENT_DATE THEN 'Oui' ELSE 'Non' END AS CAL_EST_AUJOURD_HUI,
    CASE WHEN date_complete = CURRENT_DATE - 1 DAY THEN 'Oui' ELSE 'Non' END AS CAL_EST_HIER,
    CASE WHEN date_complete = CURRENT_DATE + 1 DAY THEN 'Oui' ELSE 'Non' END AS CAL_EST_DEMAIN,
    CASE WHEN MONTH(date_complete) = MONTH(CURRENT_DATE)
              AND YEAR(date_complete) = YEAR(CURRENT_DATE)
         THEN 'Oui' ELSE 'Non' END AS CAL_EST_MOIS_COURANT,
    CASE WHEN YEAR(date_complete) = YEAR(CURRENT_DATE) THEN 'Oui' ELSE 'Non' END AS CAL_EST_ANNEE_COURANTE,
    CASE WHEN date_complete <= CURRENT_DATE THEN 'Oui' ELSE 'Non' END AS CAL_EST_PASSE,
    CASE WHEN date_complete > CURRENT_DATE THEN 'Oui' ELSE 'Non' END AS CAL_EST_FUTUR,

    -- Début et fin de période
    DATE(CAST(YEAR(date_complete) AS VARCHAR(4)) || '-01-01') AS CAL_DEBUT_ANNEE,
    DATE(CAST(YEAR(date_complete) AS VARCHAR(4)) || '-12-31') AS CAL_FIN_ANNEE,
    DATE(CAST(YEAR(date_complete) AS VARCHAR(4)) || '-' || RIGHT('0' || CAST(MONTH(date_complete) AS VARCHAR(2)), 2) || '-01') AS CAL_DEBUT_MOIS,
    LAST_DAY(date_complete) AS CAL_FIN_MOIS,

    -- Informations relatives
    date_complete - 1 DAY AS CAL_JOUR_PRECEDENT,
    date_complete + 1 DAY AS CAL_JOUR_SUIVANT,
    DAYS(CURRENT_DATE) - DAYS(date_complete) AS CAL_JOURS_DEPUIS_AUJOURD_HUI,

    -- Semestre
    CASE
        WHEN MONTH(date_complete) <= 6 THEN 1
        ELSE 2
    END AS CAL_SEMESTRE,
    'S' || CASE WHEN MONTH(date_complete) <= 6 THEN '1' ELSE '2' END || ' ' || CAST(YEAR(date_complete) AS VARCHAR(4)) AS CAL_SEMESTRE_LIB,

    -- Format d'affichage
    TO_CHAR(date_complete, 'DD/MM/YYYY') AS CAL_DATE_FR,
    TO_CHAR(date_complete, 'YYYY-MM-DD') AS CAL_DATE_ISO,
    TO_CHAR(date_complete, 'YYYYMMDD') AS CAL_DATE_NUM

FROM calendar
ORDER BY date_complete
