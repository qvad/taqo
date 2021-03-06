DROP TABLE IF EXISTS t1000000;
DROP TABLE IF EXISTS t500000;
DROP TABLE IF EXISTS t50000;
DROP TABLE IF EXISTS t100;

CREATE TABLE t1000000 as select a, md5(random()::text) from generate_Series(1,1000000) a;
CREATE TABLE t500000 as select a, md5(random()::text) from generate_Series(1,500000) a;
CREATE TABLE t50000 as select a, md5(random()::text) from generate_Series(1,50000) a;
CREATE TABLE t100 as select a, md5(random()::text) from generate_Series(1,100) a;

CREATE INDEX t1000000_idx ON t1000000(a);
CREATE INDEX t500000_idx ON t500000(a);
CREATE INDEX t50000_idx ON t50000(a);
CREATE INDEX t100_idx ON t100(a);

ANALYZE t1000000;
ANALYZE t500000;
ANALYZE t50000;
ANALYZE t100;