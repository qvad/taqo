CREATE TABLE t1(k1 int,
                k2 text,
                v1 int,
                v2 text,
                PRIMARY KEY(k1, k2));
CREATE INDEX ON t1(v1, k2);

CREATE TABLE t2(k1 int,
                k2 text,
                v1 int,
                v2 text,
                PRIMARY KEY(k1, k2));
CREATE INDEX ON t2(v1, k2);

CREATE TABLE t3(k1 int,
                k2 text,
                v1 int,
                v2 text,
                PRIMARY KEY(k1, k2));
CREATE INDEX ON t3(v1, k2);

CREATE TABLE ts2(k1 int,
                 k2 text,
                 v1 int,
                 v2 text,
                 PRIMARY KEY(k1, k2));

CREATE TABLE ts3(k1 int,
                 k2 text,
                 v1 int,
                 v2 text,
                 PRIMARY KEY(k1));
