SELECT * FROM t1 JOIN t2 ON t1.v1 = t2.v1 WHERE t1.k1 >= %(8180) AND t1.k1 < %(8190) AND t2.k1 >= %(8180) AND t2.k1 < %(8190);
SELECT * FROM t1 JOIN t2 ON t1.v1 = t2.v1 WHERE t1.k1 >= %(8100) AND t1.k1 < %(8190) AND t2.k1 >= %(8180) AND t2.k1 < %(8400);
SELECT * FROM t2 JOIN ts2 ON t2.v1 = ts2.v1 WHERE t2.k1 >= %(17180) AND t2.k1 < %(17190) AND ts2.k1 >= %(2180) AND ts2.k1 < %(2190);
SELECT * FROM t2 JOIN ts2 ON t2.v1 = ts2.v1 WHERE t2.k1 >= %(17100) AND t2.k1 < %(17190) AND ts2.k1 >= %(2180) AND ts2.k1 < %(2400);
SELECT * FROM t2 JOIN ts3 ON t2.v1 = ts3.v1 WHERE t2.k1 >= %(17180) AND t2.k1 < %(17190) AND ts3.k1 >= %(2180) AND ts3.k1 < %(2190);
SELECT * FROM t2 JOIN ts3 ON t2.v1 = ts3.v1 WHERE t2.k1 >= %(17100) AND t2.k1 < %(17190) AND ts3.k1 >= %(2180) AND ts3.k1 < %(2400);
SELECT * FROM ts2 JOIN ts3 ON ts2.v1 = ts3.v1 WHERE ts2.k1 >= %(17180) AND ts2.k1 < %(17190) AND ts3.k1 >= %(2180) AND ts3.k1 < %(2190);
SELECT * FROM ts2 JOIN ts3 ON ts2.v1 = ts3.v1 WHERE ts2.k1 >= %(17100) AND ts2.k1 < %(17190) AND ts3.k1 >= %(2180) AND ts3.k1 < %(2400);

SELECT * FROM t1 JOIN t2 ON t1.k1 = t2.k1 WHERE t1.k1 >= %(8180) AND t1.k1 < %(8190) AND t2.k1 >= %(8180) AND t2.k1 < %(8190);
SELECT * FROM t1 JOIN t2 ON t1.k1 = t2.k1 WHERE t1.k1 >= %(8100) AND t1.k1 < %(8190) AND t2.k1 >= %(8180) AND t2.k1 < %(8400);
SELECT * FROM t2 JOIN ts2 ON t2.k1 = ts2.k1 WHERE t2.k1 >= %(17180) AND t2.k1 < %(17190) AND ts2.k1 >= %(2180) AND ts2.k1 < %(2190);
SELECT * FROM t2 JOIN ts2 ON t2.k1 = ts2.k1 WHERE t2.k1 >= %(17100) AND t2.k1 < %(17190) AND ts2.k1 >= %(2180) AND ts2.k1 < %(2400);
SELECT * FROM t2 JOIN ts3 ON t2.k1 = ts3.k1 WHERE t2.k1 >= %(17180) AND t2.k1 < %(17190) AND ts3.k1 >= %(2180) AND ts3.k1 < %(2190);
SELECT * FROM t2 JOIN ts3 ON t2.k1 = ts3.k1 WHERE t2.k1 >= %(17100) AND t2.k1 < %(17190) AND ts3.k1 >= %(2180) AND ts3.k1 < %(2400);
SELECT * FROM ts2 JOIN ts3 ON ts2.k1 = ts3.k1 WHERE ts2.k1 >= %(17180) AND ts2.k1 < %(17190) AND ts3.k1 >= %(2180) AND ts3.k1 < %(2190);
SELECT * FROM ts2 JOIN ts3 ON ts2.k1 = ts3.k1 WHERE ts2.k1 >= %(17100) AND ts2.k1 < %(17190) AND ts3.k1 >= %(2180) AND ts3.k1 < %(2400);
