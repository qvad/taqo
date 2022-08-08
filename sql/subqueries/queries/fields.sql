-- FULL JOINS
-- SUBSELECT more that some value from subquery
SELECT t1000000.a,
       (SELECT t500000.a
        FROM t500000
                 FULL JOIN t50000 ON t500000.a = t50000.a
        WHERE t500000.a < 5000 LIMIT 1)
FROM t1000000
WHERE t1000000.a < (SELECT MAX(a) from t50000)
ORDER BY t1000000.a
LIMIT 1000;

-- SUBSELECT less that some value from subquery
SELECT t1000000.a,
       (SELECT t500000.a
        FROM t500000
                 FULL JOIN t50000 ON t500000.a = t50000.a
        WHERE t500000.a > 5000 LIMIT 1)
FROM t1000000
WHERE t1000000.a > (SELECT AVG(a) from t50000)
ORDER BY t1000000.a;

-- SUBSELECT in range of values
SELECT t1000000.a,
       (SELECT t500000.a
        FROM t500000
                 FULL JOIN t50000 ON t500000.a = t50000.a
        WHERE t500000.a < 5000 LIMIT 1)
FROM t1000000
WHERE t1000000.a in
      (SELECT t50000.a from t50000 where t50000.a < 100)
ORDER BY t1000000.a
LIMIT 1000;


-- INNER JOINS
-- SUBSELECT more that some value from subquery
SELECT t1000000.a,
       (SELECT t500000.a
        FROM t500000
                 INNER JOIN t50000 ON t500000.a = t50000.a
        WHERE t500000.a < 5000 LIMIT 1)
FROM t1000000
WHERE t1000000.a < (SELECT MAX(a) from t50000)
ORDER BY t1000000.a
LIMIT 1000;

-- SUBSELECT less that some value from subquery
SELECT t1000000.a,
       (SELECT t500000.a
        FROM t500000
                 INNER JOIN t50000 ON t500000.a = t50000.a
        WHERE t500000.a > 5000 LIMIT 1)
FROM t1000000
WHERE t1000000.a > (SELECT AVG(a) from t50000)
ORDER BY t1000000.a;

-- SUBSELECT in range of values
SELECT t1000000.a,
       (SELECT t500000.a
        FROM t500000
                 INNER JOIN t50000 ON t500000.a = t50000.a
        WHERE t500000.a < 5000 LIMIT 1)
FROM t1000000
WHERE t1000000.a in
      (SELECT t50000.a from t50000 where t50000.a < 100)
ORDER BY t1000000.a
LIMIT 1000;

-- LEFT JOINS
-- SUBSELECT more that some value from subquery
SELECT t1000000.a,
       (SELECT t500000.a
        FROM t500000
                 INNER JOIN t50000 ON t500000.a = t50000.a
        WHERE t500000.a < 5000 LIMIT 1)
FROM t1000000
WHERE t1000000.a < (SELECT MAX(a) from t50000)
ORDER BY t1000000.a
LIMIT 1000;

-- SUBSELECT less that some value from subquery
SELECT t1000000.a,
       (SELECT t500000.a
        FROM t500000
                 INNER JOIN t50000 ON t500000.a = t50000.a
        WHERE t500000.a > 5000 LIMIT 1)
FROM t1000000
WHERE t1000000.a > (SELECT AVG(a) from t50000)
ORDER BY t1000000.a;

-- SUBSELECT in range of values
SELECT t1000000.a,
       (SELECT t500000.a
        FROM t500000
                 INNER JOIN t50000 ON t500000.a = t50000.a
        WHERE t500000.a < 5000 LIMIT 1)
FROM t1000000
WHERE t1000000.a in
      (SELECT t50000.a from t50000 where t50000.a < 100)
ORDER BY t1000000.a
LIMIT 1000;