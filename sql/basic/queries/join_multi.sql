SELECT * FROM t1 JOIN t2 ON t1.v1 = t2.v1 WHERE t1.k1 in (%(810), %(8180), %(820));
SELECT * FROM t1 JOIN t2 ON t1.v1 = t2.v1 WHERE t1.k1 in (%(810), %(8180), %(820), %(830), %(8380), %(840), %(850), %(8580), %(860), %(870), %(8780), %(880), %(890), %(8980), %(900));
SELECT * FROM t2 JOIN ts3 ON t2.v1 = ts3.v1 WHERE t2.k1 in (%(810), %(8180), %(820));
SELECT * FROM t2 JOIN ts3 ON t2.v1 = ts3.v1 WHERE t2.k1 in (%(810), %(8180), %(820), %(830), %(8380), %(840), %(850), %(8580), %(860), %(870), %(8780), %(880), %(890), %(8980), %(900));
SELECT * FROM ts2 JOIN ts3 ON ts2.v1 = ts3.v1 WHERE ts2.k1 in (%(810), %(8180), %(820));
SELECT * FROM ts2 JOIN ts3 ON ts2.v1 = ts3.v1 WHERE ts2.k1 in (%(810), %(8180), %(820), %(830), %(8380), %(840), %(850), %(8580), %(860), %(870), %(8780), %(880), %(890), %(8980), %(900));

SELECT * FROM t1 LEFT JOIN t2 ON t1.v1 = t2.v1 WHERE t1.k1 in (%(810), %(8180), %(820));
SELECT * FROM t1 LEFT JOIN t2 ON t1.v1 = t2.v1 WHERE t1.k1 in (%(810), %(8180), %(820), %(830), %(8380), %(840), %(850), %(8580), %(860), %(870), %(8780), %(880), %(890), %(8980), %(900));
SELECT * FROM t2 LEFT JOIN ts3 ON t2.v1 = ts3.v1 WHERE t2.k1 in (%(810), %(8180), %(820));
SELECT * FROM t2 LEFT JOIN ts3 ON t2.v1 = ts3.v1 WHERE t2.k1 in (%(810), %(8180), %(820), %(830), %(8380), %(840), %(850), %(8580), %(860), %(870), %(8780), %(880), %(890), %(8980), %(900));
SELECT * FROM ts2 LEFT JOIN ts3 ON ts2.v1 = ts3.v1 WHERE ts2.k1 in (%(810), %(8180), %(820));
SELECT * FROM ts2 LEFT JOIN ts3 ON ts2.v1 = ts3.v1 WHERE ts2.k1 in (%(810), %(8180), %(820), %(830), %(8380), %(840), %(850), %(8580), %(860), %(870), %(8780), %(880), %(890), %(8980), %(900));

SELECT * FROM t1 RIGHT JOIN t2 ON t1.v1 = t2.v1 WHERE t1.k1 in (%(810), %(8180), %(820));
SELECT * FROM t1 RIGHT JOIN t2 ON t1.v1 = t2.v1 WHERE t1.k1 in (%(810), %(8180), %(820), %(830), %(8380), %(840), %(850), %(8580), %(860), %(870), %(8780), %(880), %(890), %(8980), %(900));
SELECT * FROM t2 RIGHT JOIN ts3 ON t2.v1 = ts3.v1 WHERE t2.k1 in (%(810), %(8180), %(820));
SELECT * FROM t2 RIGHT JOIN ts3 ON t2.v1 = ts3.v1 WHERE t2.k1 in (%(810), %(8180), %(820), %(830), %(8380), %(840), %(850), %(8580), %(860), %(870), %(8780), %(880), %(890), %(8980), %(900));
SELECT * FROM ts2 RIGHT JOIN ts3 ON ts2.v1 = ts3.v1 WHERE ts2.k1 in (%(810), %(8180), %(820));
SELECT * FROM ts2 RIGHT JOIN ts3 ON ts2.v1 = ts3.v1 WHERE ts2.k1 in (%(810), %(8180), %(820), %(830), %(8380), %(840), %(850), %(8580), %(860), %(870), %(8780), %(880), %(890), %(8980), %(900));

SELECT * FROM t1 FULL JOIN t2 ON t1.v1 = t2.v1 WHERE t1.k1 in (%(810), %(8180), %(820));
SELECT * FROM t1 FULL JOIN t2 ON t1.v1 = t2.v1 WHERE t1.k1 in (%(810), %(8180), %(820), %(830), %(8380), %(840), %(850), %(8580), %(860), %(870), %(8780), %(880), %(890), %(8980), %(900));
SELECT * FROM t2 FULL JOIN ts3 ON t2.v1 = ts3.v1 WHERE t2.k1 in (%(810), %(8180), %(820));
SELECT * FROM t2 FULL JOIN ts3 ON t2.v1 = ts3.v1 WHERE t2.k1 in (%(810), %(8180), %(820), %(830), %(8380), %(840), %(850), %(8580), %(860), %(870), %(8780), %(880), %(890), %(8980), %(900));
SELECT * FROM ts2 FULL JOIN ts3 ON ts2.v1 = ts3.v1 WHERE ts2.k1 in (%(810), %(8180), %(820));
SELECT * FROM ts2 FULL JOIN ts3 ON ts2.v1 = ts3.v1 WHERE ts2.k1 in (%(810), %(8180), %(820), %(830), %(8380), %(840), %(850), %(8580), %(860), %(870), %(8780), %(880), %(890), %(8980),%(900));

SELECT * FROM t1 JOIN t2 ON t1.k1 = t2.k1 WHERE t1.k1 in (%(810), %(8180), %(820));
SELECT * FROM t1 JOIN t2 ON t1.k1 = t2.k1 WHERE t1.k1 in (%(810), %(8180), %(820), %(830), %(8380), %(840), %(850), %(8580), %(860), %(870), %(8780), %(880), %(890), %(8980),%(900));
SELECT * FROM t2 JOIN ts3 ON t2.k1 = ts3.k1 WHERE t2.k1 in (%(810), %(8180), %(820));
SELECT * FROM t2 JOIN ts3 ON t2.k1 = ts3.k1 WHERE t2.k1 in (%(810), %(8180), %(820), %(830), %(8380), %(840), %(850), %(8580), %(860), %(870), %(8780), %(880), %(890), %(8980),%(900));
SELECT * FROM ts2 JOIN ts3 ON ts2.k1 = ts3.k1 WHERE ts2.k1 in (%(810), %(8180), %(820));
SELECT * FROM ts2 JOIN ts3 ON ts2.k1 = ts3.k1 WHERE ts2.k1 in (%(810), %(8180), %(820), %(830), %(8380), %(840), %(850), %(8580), %(860), %(870), %(8780), %(880), %(890), %(8980),%(900));

SELECT * FROM t1 LEFT JOIN t2 ON t1.k1 = t2.k1 WHERE t1.k1 in (%(810), %(8180), %(820));
SELECT * FROM t1 LEFT JOIN t2 ON t1.k1 = t2.k1 WHERE t1.k1 in (%(810), %(8180), %(820), %(830), %(8380), %(840), %(850), %(8580), %(860), %(870), %(8780), %(880), %(890), %(8980),%(900));
SELECT * FROM t2 LEFT JOIN ts3 ON t2.k1 = ts3.k1 WHERE t2.k1 in (%(810), %(8180), %(820));
SELECT * FROM t2 LEFT JOIN ts3 ON t2.k1 = ts3.k1 WHERE t2.k1 in (%(810), %(8180), %(820), %(830), %(8380), %(840), %(850), %(8580), %(860), %(870), %(8780), %(880), %(890), %(8980),%(900));
SELECT * FROM ts2 LEFT JOIN ts3 ON ts2.k1 = ts3.k1 WHERE ts2.k1 in (%(810), %(8180), %(820));
SELECT * FROM ts2 LEFT JOIN ts3 ON ts2.k1 = ts3.k1 WHERE ts2.k1 in (%(810), %(8180), %(820), %(830), %(8380), %(840), %(850), %(8580), %(860), %(870), %(8780), %(880), %(890), %(8980),%(900));

SELECT * FROM t1 RIGHT JOIN t2 ON t1.k1 = t2.k1 WHERE t1.k1 in (%(810), %(8180), %(820));
SELECT * FROM t1 RIGHT JOIN t2 ON t1.k1 = t2.k1 WHERE t1.k1 in (%(810), %(8180), %(820), %(830), %(8380), %(840), %(850), %(8580), %(860), %(870), %(8780), %(880), %(890), %(8980),%(900));
SELECT * FROM t2 RIGHT JOIN ts3 ON t2.k1 = ts3.k1 WHERE t2.k1 in (%(810), %(8180), %(820));
SELECT * FROM t2 RIGHT JOIN ts3 ON t2.k1 = ts3.k1 WHERE t2.k1 in (%(810), %(8180), %(820), %(830), %(8380), %(840), %(850), %(8580), %(860), %(870), %(8780), %(880), %(890), %(8980),%(900));
SELECT * FROM ts2 RIGHT JOIN ts3 ON ts2.k1 = ts3.k1 WHERE ts2.k1 in (%(810), %(8180), %(820));
SELECT * FROM ts2 RIGHT JOIN ts3 ON ts2.k1 = ts3.k1 WHERE ts2.k1 in (%(810), %(8180), %(820), %(830), %(8380), %(840), %(850), %(8580), %(860), %(870), %(8780), %(880), %(890), %(8980),%(900));

SELECT * FROM t1 FULL JOIN t2 ON t1.k1 = t2.k1 WHERE t1.k1 in (%(810), %(8180), %(820));
SELECT * FROM t1 FULL JOIN t2 ON t1.k1 = t2.k1 WHERE t1.k1 in (%(810), %(8180), %(820), %(830), %(8380), %(840), %(850), %(8580), %(860), %(870), %(8780), %(880), %(890), %(8980),%(900));
SELECT * FROM t2 FULL JOIN ts3 ON t2.k1 = ts3.k1 WHERE t2.k1 in (%(810), %(8180), %(820));
SELECT * FROM t2 FULL JOIN ts3 ON t2.k1 = ts3.k1 WHERE t2.k1 in (%(810), %(8180), %(820), %(830), %(8380), %(840), %(850), %(8580), %(860), %(870), %(8780), %(880), %(890), %(8980),%(900));
SELECT * FROM ts2 FULL JOIN ts3 ON ts2.k1 = ts3.k1 WHERE ts2.k1 in (%(810), %(8180), %(820));
SELECT * FROM ts2 FULL JOIN ts3 ON ts2.k1 = ts3.k1 WHERE ts2.k1 in (%(810), %(8180), %(820), %(830), %(8380), %(840), %(850), %(8580), %(860), %(870), %(8780), %(880), %(890), %(8980),%(900));