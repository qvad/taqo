DROP TABLE IF EXISTS t1 CASCADE;
DROP TABLE IF EXISTS t2 CASCADE;
DROP TABLE IF EXISTS t3 CASCADE;
DROP TABLE IF EXISTS ts2 CASCADE;
DROP TABLE IF EXISTS ts3 CASCADE;

CREATE TABLE t1 as select k1, 'k2-' || k1 as k2, k1 as v1, md5(random()::text) as v2
                   from generate_Series(1, 50000000) k1;
CREATE INDEX ON t1 (v1 ASC, k2 ASC);

CREATE TABLE t2 as select k1, 'k2-' || k1 as k2, k1 as v1, repeat(md5(random()::text), 10) as v2
                   from generate_Series(1, 50000000) k1;
CREATE INDEX ON t2 (v1 ASC, k2 ASC);

CREATE TABLE t3 as select k1, 'k2-' || k1 as k2, k1 as v1, repeat(md5(random()::text), 50) as v2
                   from generate_Series(1, 50000000) k1;
CREATE INDEX ON t3 (v1 ASC, k2 ASC);

CREATE TABLE ts2 as select k1, 'k2-' || k1 as k2, k1 as v1, repeat(md5(random()::text), 10) as v2
                    from generate_Series(1, 20000000) k1;
CREATE INDEX ON ts2 (v1 ASC, k2 ASC);

CREATE TABLE ts3 as
    select k1, 'k2-' || k1 as k2, k1 as v1, repeat(md5(random()::text), 50) as v2
    from generate_Series(1, 5000000) k1;
CREATE INDEX ON t3 (v1 ASC, k2 ASC);
