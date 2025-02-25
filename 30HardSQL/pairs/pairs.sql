--------------------------------------------------------------------------------------------------
--PAIRS
--------------------------------------------------------------------------------------------------
CREATE TABLE PAIRS (
    BRAND1 STRING,
    BRAND2 STRING,
    YEAR INT64,
    CUSTOM1 STRING,
    CUSTOM2 STRING,
    CUSTOM3 STRING,
    CUSTOM4 STRING
);

INSERT INTO PAIRS (BRAND1, BRAND2, YEAR, CUSTOM1, CUSTOM2, CUSTOM3, CUSTOM4)
VALUES
    ('apple', 'samsung', 2020, '1', '2', '1', '2'),
    ('samsung', 'apple', 2020, '1', '2', '1', '2'),
    ('apple', 'samsung', 2021, '1', '2', '5', '3'),
    ('apple', 'samsung', 2021, '5', '3', '1', '2'),
    ('google', NULL, 2020, '5', '9', NULL, NULL),
    ('oneplus', 'nothing', 2020, '5', '9', '6', '3');
--------------------------------------------------------------------------------------------------
--PAIRS ANSWER
--------------------------------------------------------------------------------------------------
SELECT BRAND1, BRAND2, YEAR, custom1, custom2, custom3, custom4
FROM(select * 
         , ROW_NUMBER() OVER(PARTITION BY pair_id ORDER BY pair_id ASC) AS ROWX
    from(SELECT *
             , CASE WHEN p1.BRAND1 = p2.BRAND2 THEN CONCAT('pair',YEAR) ELSE CONCAT('notpair',YEAR) END AS pair_id
             , '' AS custom3_std
         	 , '' AS custom4_std
        FROM PAIRS p1
        LEFT JOIN(SELECT DISTINCT BRAND1 AS BRAND1, BRAND2 AS BRAND2 FROM PAIRS
                  ) p2
        ON p1.BRAND2 = p2.BRAND1
        ) x
     ) y
WHERE y.ROWX = 1
AND custom1 = custom3
AND custom2 = custom4

UNION ALL

SELECT BRAND1, BRAND2, YEAR, custom1, custom2, custom3, custom4
FROM(select * 
         , ROW_NUMBER() OVER(PARTITION BY pair_id ORDER BY pair_id ASC) AS ROWX
    from(SELECT *
             , CASE WHEN p1.BRAND1 = p2.BRAND2 THEN CONCAT('pair',YEAR) ELSE CONCAT('notpair',YEAR) END AS pair_id
         	 , CASE WHEN custom3 IS NULL THEN '' ELSE custom3 END AS custom3_std
         	 , CASE WHEN custom4 IS NULL THEN '' ELSE custom4 END AS custom4_std
        FROM PAIRS p1
        LEFT JOIN(SELECT DISTINCT BRAND1 AS BRAND1, BRAND2 AS BRAND2 FROM PAIRS
                  ) p2
        ON p1.BRAND2 = p2.BRAND1
        ) x
     ) y
WHERE custom1 <> custom3_std
AND custom2 <> custom4_std;
--------------------------------------------------------------------------------------------------
