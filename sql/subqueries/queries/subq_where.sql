-- FULL JOINS
-- WHERE SUBSELECT more that some value from subquery
SELECT *
FROM t1000000
         FULL JOIN t500000 ON t1000000.c_int = t500000.c_int
         FULL JOIN t50000 ON t1000000.c_int = t50000.c_int
WHERE t1000000.c_int < (SELECT MAX(c_real) from t50000 as t50k)
ORDER BY t1000000.c_int
LIMIT 1000;

-- WHERE SUBSELECT less that some value from subquery
SELECT *
FROM t1000000
         FULL JOIN t500000 ON t1000000.c_int = t500000.c_int
         FULL JOIN t50000 ON t1000000.c_int = t50000.c_int
WHERE t1000000.c_int > (SELECT AVG(c_real) from t50000 as t50k)
ORDER BY t1000000.c_int;

-- WHERE SUBSELECT in range of values
SELECT *
FROM t1000000
         FULL JOIN t500000 ON t1000000.c_int = t500000.c_int
         FULL join t50000 ON t1000000.c_int = t50000.c_int
WHERE t1000000.c_int in
      (SELECT t50000.c_int from t50000 as t50k where t50000.c_int < %(100))
ORDER BY t1000000.c_int
LIMIT 1000;

-- INNER JOINS
-- WHERE SUBSELECT more that some value from subquery
SELECT *
FROM t1000000
         INNER JOIN t500000 ON t1000000.c_int = t500000.c_int
         INNER JOIN t50000 ON t1000000.c_int = t50000.c_int
WHERE t1000000.c_int < (SELECT MAX(c_real) from t50000 as t50k)
ORDER BY t1000000.c_int
LIMIT 1000;

-- WHERE SUBSELECT less that some value from subquery
SELECT *
FROM t1000000
         INNER JOIN t500000 ON t1000000.c_int = t500000.c_int
         INNER JOIN t50000 ON t1000000.c_int = t50000.c_int
WHERE t1000000.c_int > (SELECT AVG(c_real) from t50000 as t50k)
ORDER BY t1000000.c_float;

-- WHERE SUBSELECT in range of values
SELECT *
FROM t1000000
         INNER JOIN t500000 ON t1000000.c_int = t500000.c_int
         INNER join t50000 ON t1000000.c_int = t50000.c_int
WHERE t1000000.c_int in
      (SELECT t50000.c_int from t50000 as t50k where t50000.c_real < %(100))
ORDER BY t1000000.c_float
LIMIT 1000;

-- LEFT JOINS
-- WHERE SUBSELECT more that some value from subquery
SELECT *
FROM t1000000
         LEFT OUTER JOIN t500000 ON t1000000.c_int = t500000.c_int
         LEFT OUTER JOIN t50000 ON t1000000.c_int = t50000.c_int
WHERE t1000000.c_int < (SELECT MAX(c_real) from t50000 as t50k)
ORDER BY t1000000.c_float
LIMIT 1000;

-- WHERE SUBSELECT less that some value from subquery
SELECT *
FROM t1000000
         LEFT OUTER JOIN t500000 ON t1000000.c_int = t500000.c_int
         LEFT OUTER JOIN t50000 ON t1000000.c_int = t50000.c_int
WHERE t1000000.c_int > (SELECT AVG(c_real) from t50000 as t50k)
ORDER BY t1000000.c_int;

-- WHERE SUBSELECT in range of values
SELECT *
FROM t1000000
         LEFT OUTER JOIN t500000 ON t1000000.c_int = t500000.c_int
         LEFT OUTER JOIN t50000 ON t1000000.c_int = t50000.c_int
WHERE t1000000.c_int in
      (SELECT t50k.c_int from t50000 as t50k where t50000.c_real < %(100))
ORDER BY t1000000.c_float
LIMIT 1000;