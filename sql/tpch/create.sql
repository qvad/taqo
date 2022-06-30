DROP TABLE IF EXISTS supplier CASCADE;
DROP TABLE IF EXISTS part CASCADE;
DROP TABLE IF EXISTS partsupp CASCADE;
DROP TABLE IF EXISTS customer CASCADE;
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS lineitem CASCADE;
DROP TABLE IF EXISTS nation CASCADE;
DROP TABLE IF EXISTS region CASCADE;

CREATE TABLE supplier (
        s_suppkey  INTEGER NOT NULL,
        s_name CHAR(25) NOT NULL,
        s_address VARCHAR(40) NOT NULL,
        s_nationkey INTEGER NOT NULL,
        s_phone CHAR(15) NOT NULL,
        s_acctbal NUMERIC NOT NULL,
        s_comment VARCHAR(101) NOT NULL
);

CREATE TABLE part (
        p_partkey INTEGER NOT NULL,
        p_name VARCHAR(55) NOT NULL,
        p_mfgr CHAR(25) NOT NULL,
        p_brand CHAR(10) NOT NULL,
        p_type VARCHAR(25) NOT NULL,
        p_size INTEGER NOT NULL,
        p_container CHAR(10) NOT NULL,
        p_retailprice NUMERIC NOT NULL,
        p_comment VARCHAR(23) NOT NULL
);

CREATE TABLE partsupp (
        ps_partkey INTEGER NOT NULL,
        ps_suppkey INTEGER NOT NULL,
        ps_availqty INTEGER NOT NULL,
        ps_supplycost NUMERIC NOT NULL,
        ps_comment VARCHAR(199) NOT NULL
);

CREATE TABLE customer (
        c_custkey INTEGER NOT NULL,
        c_name VARCHAR(25) NOT NULL,
        c_address VARCHAR(40) NOT NULL,
        c_nationkey INTEGER NOT NULL,
        c_phone CHAR(15) NOT NULL,
        c_acctbal NUMERIC NOT NULL,
        c_mktsegment CHAR(10) NOT NULL,
        c_comment VARCHAR(117) NOT NULL
);

CREATE TABLE orders (
        o_orderkey BIGINT NOT NULL,
        o_custkey INTEGER NOT NULL,
        o_orderstatus CHAR(1) NOT NULL,
        o_totalprice NUMERIC NOT NULL,
        o_orderdate DATE NOT NULL,
        o_orderpriority CHAR(15) NOT NULL,
        o_clerk CHAR(15) NOT NULL,
        o_shippriority INTEGER NOT NULL,
        o_comment VARCHAR(79) NOT NULL
);

CREATE TABLE lineitem (
        l_orderkey BIGINT NOT NULL,
        l_partkey INTEGER NOT NULL,
        l_suppkey INTEGER NOT NULL,
        l_linenumber INTEGER NOT NULL,
        l_quantity NUMERIC NOT NULL,
        l_extendedprice NUMERIC NOT NULL,
        l_discount NUMERIC NOT NULL,
        l_tax NUMERIC NOT NULL,
        l_returnflag CHAR(1) NOT NULL,
        l_linestatus CHAR(1) NOT NULL,
        l_shipdate DATE NOT NULL,
        l_commitdate DATE NOT NULL,
        l_receiptdate DATE NOT NULL,
        l_shipinstruct CHAR(25) NOT NULL,
        l_shipmode CHAR(10) NOT NULL,
        l_comment VARCHAR(44) NOT NULL
);

CREATE TABLE nation (
        n_nationkey INTEGER NOT NULL,
        n_name CHAR(25) NOT NULL,
        n_regionkey INTEGER NOT NULL,
        n_comment VARCHAR(152) NOT NULL
);

CREATE TABLE region (
        r_regionkey INTEGER NOT NULL,
        r_name CHAR(25) NOT NULL,
        r_comment VARCHAR(152) NOT NULL
);

CREATE INDEX supplier_s_suppkey_idx_like ON supplier (s_suppkey) WHERE s_comment LIKE '%Customer%Complaints%';
CREATE INDEX supplier_s_nationkey_s_suppkey_idx ON supplier (s_nationkey, s_suppkey);
CREATE INDEX part_p_type_p_partkey_idx ON part(p_type, p_partkey);
CREATE INDEX part_p_container_p_brand_p_partkey_idx ON part(p_container, p_brand, p_partkey);
CREATE INDEX part_p_size_idx ON part(p_size);
CREATE INDEX part_p_name_idx ON part(p_name);
CREATE INDEX partsupp_ps_suppkey_idx ON partsupp (ps_suppkey);
CREATE INDEX customer_c_nationkey_c_custkey_idx ON customer (c_nationkey, c_custkey);
CREATE INDEX customer_c_phone_idx_c_acctbal ON customer (substr(c_phone::text, 1, 2)) WHERE c_acctbal > 0.00;
CREATE INDEX customer_c_phone_idx ON customer (substr(c_phone::text, 1, 2), c_acctbal);
CREATE INDEX customer_c_mktsegment_c_custkey_idx ON customer (c_mktsegment, c_custkey);
CREATE INDEX orders_o_orderdate_o_orderkey_idx ON orders (o_orderdate, o_orderkey);
CREATE INDEX orders_o_orderkey_o_orderdate_idx ON orders (o_orderkey, o_orderdate);
CREATE INDEX lineitem_l_partkey_l_quantity_l_shipmode_idx ON lineitem (l_partkey, l_quantity, l_shipmode);
CREATE INDEX lineitem_l_orderkey_idx ON lineitem (l_orderkey);
CREATE INDEX lineitem_l_orderkey_idx_l_returnflag ON lineitem (l_orderkey) WHERE l_returnflag = 'R';
CREATE INDEX lineitem_l_orderkey_idx_part1 ON lineitem (l_orderkey) WHERE l_commitdate < l_receiptdate;
CREATE INDEX lineitem_l_orderkey_idx_part2 ON lineitem (l_orderkey) WHERE l_commitdate < l_receiptdate AND l_shipdate < l_commitdate;
CREATE INDEX lineitem_l_shipdate_l_suppkey__idx ON lineitem (l_shipdate, l_suppkey);
CREATE INDEX lineitem_l_orderkey_l_linenumber_l_shipdate_idx ON lineitem (l_orderkey, l_linenumber, l_shipdate);

ALTER TABLE supplier ADD CONSTRAINT pk_supplier PRIMARY KEY (s_suppkey);
ALTER TABLE part ADD CONSTRAINT pk_part PRIMARY KEY (p_partkey);
ALTER TABLE partsupp ADD CONSTRAINT pk_partsupp PRIMARY KEY (ps_partkey, ps_suppkey);
ALTER TABLE customer ADD CONSTRAINT pk_customer PRIMARY KEY (c_custkey);
ALTER TABLE orders ADD CONSTRAINT pk_orders PRIMARY KEY (o_orderkey);
ALTER TABLE lineitem ADD CONSTRAINT pk_lineitem PRIMARY KEY (l_orderkey, l_linenumber);
ALTER TABLE nation ADD CONSTRAINT pk_nation PRIMARY KEY (n_nationkey);
ALTER TABLE region ADD CONSTRAINT pk_region PRIMARY KEY (r_regionkey);
