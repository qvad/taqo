-- FULL JOINS
-- SUBSELECT more that some value from subquery
SELECT t1000000.c_text,
       (SELECT t500000.c_money
        FROM t500000
                 FULL JOIN t50000 ON t500000.c_int = t50000.c_int
        WHERE t500000.c_float < %(5000) LIMIT 1)
FROM t1000000
WHERE t1000000.c_int < (SELECT MAX(c_real) from t50000 as t50k)
ORDER BY t1000000.c_decimal
LIMIT 1000;

-- SUBSELECT less that some value from subquery
SELECT t1000000.c_text,
       (SELECT t500000.c_money
        FROM t500000
                 FULL JOIN t50000 ON t500000.c_int = t50000.c_int
        WHERE t500000.c_float > %(5000) LIMIT 1)
FROM t1000000
WHERE t1000000.c_real > (SELECT AVG(c_real) from t50000 as t50k)
ORDER BY t1000000.c_decimal;

-- SUBSELECT in range of values
SELECT t1000000.c_text,
       (SELECT t500000.c_money
        FROM t500000
                 FULL JOIN t50000 ON t500000.c_int = t50000.c_int
        WHERE t500000.c_float < %(5000) LIMIT 1)
FROM t1000000
WHERE t1000000.c_real in
      (SELECT t50k.c_real from t50000 as t50k where t50000.c_real < %(100))
ORDER BY t1000000.c_decimal
LIMIT 1000;

-- SUBSELECT less that some value from subquery
SELECT t1000000.c_text,
       (SELECT t500000.c_money
        FROM t500000
                 INNER JOIN t50000 ON t500000.c_int = t50000.c_int
        WHERE t500000.c_float > %(5000) LIMIT 1)
FROM t1000000
WHERE t1000000.c_real > (SELECT AVG(c_real) from t50000 as t50k)
ORDER BY t1000000.c_decimal;

-- LEFT JOINS
-- SUBSELECT more that some value from subquery
SELECT t1000000.c_text,
       (SELECT t500000.c_money
        FROM t500000
                 INNER JOIN t50000 ON t500000.c_int = t50000.c_int
        WHERE t500000.c_float < %(5000) LIMIT 1)
FROM t1000000
WHERE t1000000.c_real < (SELECT MAX(c_real) from t50000 as t50k)
ORDER BY t1000000.c_decimal
LIMIT 1000;

-- SUBSELECT in range of values
SELECT t1000000.c_text,
       (SELECT t500000.c_money
        FROM t500000
                 INNER JOIN t50000 ON t500000.c_int = t50000.c_int
        WHERE t500000.c_float < %(5000) LIMIT 1)
FROM t1000000
WHERE t1000000.c_real in
      (SELECT t50k.c_real from t50000 as t50k where t50000.c_real < %(100))
ORDER BY t1000000.c_decimal
LIMIT 1000;