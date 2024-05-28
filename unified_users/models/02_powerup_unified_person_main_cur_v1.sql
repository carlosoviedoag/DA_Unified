{{ config(
    materialized = 'table',
    schema = 'customer_pl'
) }}

WITH first_query AS (
    SELECT CASE WHEN pmc.unified_status = 'primary' THEN pmc.user_id
                WHEN ccm.unified_status = 'primary' THEN ccm.user_id
                END AS unique_user_id,
           ccm.user_id,      
           CASE WHEN pmc.created_tms > ccm.created_tms THEN pmc.name ELSE ccm.name END AS name,
           CASE WHEN pmc.created_tms > ccm.created_tms THEN pmc.first_lastname ELSE ccm.first_lastname END AS first_lastname,
           CASE WHEN pmc.created_tms > ccm.created_tms THEN pmc.second_lastname ELSE ccm.second_lastname END AS second_lastname,
           CASE WHEN pmc.created_tms > ccm.created_tms THEN pmc.email ELSE ccm.email END AS email,
           CASE WHEN pmc.created_tms > ccm.created_tms THEN pmc.gender ELSE ccm.gender END AS gender,
           CASE WHEN pmc.created_tms > ccm.created_tms THEN pmc.birth_date ELSE ccm.birth_date END AS birth_date,
           CASE WHEN pmc.created_tms > ccm.created_tms THEN pmc.rfc ELSE ccm.rfc END AS rfc,
           CASE WHEN pmc.created_tms > ccm.created_tms THEN pmc.curp ELSE ccm.curp END AS curp,
           CASE WHEN pmc.created_tms > ccm.created_tms THEN pmc.nationality ELSE ccm.nationality END AS nationality,
           CASE WHEN pmc.created_tms > ccm.created_tms THEN pmc.created_tms ELSE ccm.created_tms END AS created_tms
    FROM {{source('stori_ccm', 'person_main_cur')}} ccm 
    INNER JOIN {{source('rds_person', 'main_cur')}} pmc ON pmc.cognito_user_id = ccm.cognito_user_id
)


SELECT * FROM first_query

UNION 

SELECT '' AS unique_user_id,
           ccm.user_id,
       ccm.name AS name,
       ccm.first_lastname AS first_lastname,
       ccm.second_lastname AS second_lastname,
       ccm.email AS email,
       ccm.gender AS gender,
       ccm.birth_date AS birth_date,
       ccm.rfc AS rfc,
       ccm.curp AS curp,
       ccm.nationality AS nationality,
       ccm.created_tms AS created_tms
FROM {{source('stori_ccm', 'person_main_cur')}} ccm
WHERE ccm.user_id NOT IN (SELECT user_id FROM first_query)