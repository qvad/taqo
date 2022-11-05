SELECT t1.k1, t1.k2, t2.v1, t2.v2 FROM t1 JOIN t2 on t1.k1 = t2.k1 WHERE t1.k1 >= %(2500) AND t1.k1 < %(25100) AND t2.k1 >= %(2500) AND t2.k1 < %(25100) GROUP BY t1.k1, t1.k2, t2.v1, t2.v2;
SELECT t1.k1, t1.k2, t2.v1, t2.v2 FROM t1 JOIN t2 on t1.k1 = t2.k1 WHERE t1.k1 >= %(24800) AND t1.k1 < %(25100) AND t2.k1 >= %(2500) AND t2.k1 < %(25300) GROUP BY t1.k1, t1.k2, t2.v1, t2.v2;
SELECT ts2.k1, ts2.k2, ts3.v1, ts3.v2 FROM ts2 JOIN ts3 on ts2.k1 = ts3.k1 WHERE ts2.k1 >= %(300) AND ts2.k1 < %(3100) AND ts3.k1 >= %(300) AND ts3.k1 < %(3100) GROUP BY ts2.k1, ts2.k2, ts3.v1, ts3.v2;
SELECT ts2.k1, ts2.k2, ts3.v1, ts3.v2 FROM ts2 JOIN ts3 on ts2.k1 = ts3.k1 WHERE ts2.k1 >= %(2800) AND ts2.k1 < %(3100) AND ts3.k1 >= %(300) AND ts3.k1 < %(3300) GROUP BY ts2.k1, ts2.k2, ts3.v1, ts3.v2;