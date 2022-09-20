CREATE EXTENSION parquet_fdw
;

CREATE SERVER parquet_srv FOREIGN DATA WRAPPER parquet_fdw;
CREATE USER MAPPING FOR postgres SERVER parquet_srv OPTIONS (user 'postgres');

DROP TABLE IF EXISTS nation_external CASCADE;
DROP TABLE IF EXISTS region CASCADE;
DROP TABLE IF EXISTS part CASCADE;
DROP TABLE IF EXISTS supplier CASCADE;
DROP TABLE IF EXISTS partsupp CASCADE;
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS customer CASCADE;
DROP TABLE IF EXISTS lineitem CASCADE;

CREATE FOREIGN TABLE region_external (
    r_regionkey INTEGER NOT NULL
    , r_name CHAR(25) NOT NULL
    , r_comment VARCHAR(152)
    )
    SERVER parquet_srv
    OPTIONS (
        filename '/var/lib/postgresql/data/tpch/1/region/region.parquet'
        );

CREATE FOREIGN TABLE nation_external
    (
        n_nationkey INTEGER NOT NULL,
        n_name CHAR(25) NOT NULL,
        n_regionkey INTEGER NOT NULL,
        n_comment VARCHAR(152)
        )
    SERVER parquet_srv
    OPTIONS (
        filename '/var/lib/postgresql/data/tpch/1/nation/nation.parquet'
        );

CREATE FOREIGN TABLE part_external
    (
        p_partkey INTEGER NOT NULL,
        p_name VARCHAR(55) NOT NULL,
        p_mfgr CHAR(25) NOT NULL,
        p_brand CHAR(10) NOT NULL,
        p_type VARCHAR(25) NOT NULL,
        p_size INTEGER NOT NULL,
        p_container CHAR(10) NOT NULL,
        p_retailprice DECIMAL(15, 2) NOT NULL,
        p_comment VARCHAR(23) NOT NULL
        )
    SERVER parquet_srv
    OPTIONS (
        filename '/var/lib/postgresql/data/tpch/1/part/part.1.parquet'
        );

CREATE FOREIGN TABLE supplier_external
    (
        s_suppkey INTEGER NOT NULL,
        s_name CHAR(25) NOT NULL,
        s_address VARCHAR(40) NOT NULL,
        s_nationkey INTEGER NOT NULL,
        s_phone CHAR(15) NOT NULL,
        s_acctbal DECIMAL(15, 2) NOT NULL,
        s_comment VARCHAR(101) NOT NULL
        )
    SERVER parquet_srv
    OPTIONS (
        filename '/var/lib/postgresql/data/tpch/1/supplier/supplier.1.parquet'
        );

CREATE FOREIGN TABLE partsupp_external (
    ps_partkey INTEGER NOT NULL,
    ps_suppkey INTEGER NOT NULL,
    ps_availqty INTEGER NOT NULL,
    ps_supplycost DECIMAL(15, 2) NOT NULL,
    ps_comment VARCHAR(199) NOT NULL
    )
    SERVER parquet_srv
    OPTIONS (
        filename '/var/lib/postgresql/data/tpch/1/partsupp/partsupp.1.parquet'
        );


CREATE FOREIGN TABLE customer_external (
    c_custkey INTEGER NOT NULL,
    c_name VARCHAR(25) NOT NULL,
    c_address VARCHAR(40) NOT NULL,
    c_nationkey INTEGER NOT NULL,
    c_phone CHAR(15) NOT NULL,
    c_acctbal DECIMAL(15, 2) NOT NULL,
    c_mktsegment CHAR(10) NOT NULL,
    c_comment VARCHAR(117) NOT NULL
    )
    SERVER parquet_srv
    OPTIONS (
        filename '/var/lib/postgresql/data/tpch/1/customer/customer.1.parquet'
        );

CREATE FOREIGN TABLE orders_external (
    o_orderkey INTEGER NOT NULL,
    o_custkey INTEGER NOT NULL,
    o_orderstatus CHAR(1) NOT NULL,
    o_totalprice DECIMAL(15, 2) NOT NULL,
    o_orderdate DATE NOT NULL,
    o_orderpriority CHAR(15) NOT NULL,
    o_clerk CHAR(15) NOT NULL,
    o_shippriority INTEGER NOT NULL,
    o_comment VARCHAR(79) NOT NULL
    )
    SERVER parquet_srv
    OPTIONS (
        filename '/var/lib/postgresql/data/tpch/1/orders/orders.1.parquet'
        );

CREATE FOREIGN TABLE lineitem_external (
    l_orderkey INTEGER NOT NULL,
    l_partkey INTEGER NOT NULL,
    l_suppkey INTEGER NOT NULL,
    l_linenumber INTEGER NOT NULL,
    l_quantity DECIMAL(15, 2) NOT NULL,
    l_extendedprice DECIMAL(15, 2) NOT NULL,
    l_discount DECIMAL(15, 2) NOT NULL,
    l_tax DECIMAL(15, 2) NOT NULL,
    l_returnflag CHAR(1) NOT NULL,
    l_linestatus CHAR(1) NOT NULL,
    l_shipdate DATE NOT NULL,
    l_commitdate DATE NOT NULL,
    l_receiptdate DATE NOT NULL,
    l_shipinstruct CHAR(25) NOT NULL,
    l_shipmode CHAR(10) NOT NULL,
    l_comment VARCHAR(44) NOT NULL
    )
    SERVER parquet_srv
    OPTIONS (
        filename '/var/lib/postgresql/data/tpch/1/lineitem/lineitem.1.parquet'
        );
;

/* ----------------------------------------------------------------------- */

DROP TABLE IF EXISTS nation CASCADE;
DROP TABLE IF EXISTS region CASCADE;
DROP TABLE IF EXISTS part CASCADE;
DROP TABLE IF EXISTS supplier CASCADE;
DROP TABLE IF EXISTS partsupp CASCADE;
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS customer CASCADE;
DROP TABLE IF EXISTS lineitem CASCADE;

CREATE TABLE region (
    r_regionkey INTEGER  NOT NULL,
    r_name      CHAR(25) NOT NULL,
    r_comment   VARCHAR(152),
    PRIMARY KEY (r_regionkey)
);

INSERT INTO region (r_regionkey, r_name, r_comment)
SELECT r_regionkey, r_name, r_comment
FROM region_external;

CREATE TABLE nation (
    n_nationkey INTEGER  NOT NULL,
    n_name      CHAR(25) NOT NULL,
    n_regionkey INTEGER  NOT NULL,
    n_comment   VARCHAR(152),
    PRIMARY KEY (n_nationkey),
    FOREIGN KEY (n_regionkey) REFERENCES region (r_regionkey)
);

INSERT INTO nation (n_nationkey, n_name, n_regionkey, n_comment)
SELECT n_nationkey, n_name, n_regionkey, n_comment
FROM nation_external;

CREATE TABLE part (
    p_partkey     INTEGER        NOT NULL,
    p_name        VARCHAR(55)    NOT NULL,
    p_mfgr        CHAR(25)       NOT NULL,
    p_brand       CHAR(10)       NOT NULL,
    p_type        VARCHAR(25)    NOT NULL,
    p_size        INTEGER        NOT NULL,
    p_container   CHAR(10)       NOT NULL,
    p_retailprice DECIMAL(15, 2) NOT NULL,
    p_comment     VARCHAR(23)    NOT NULL,
    PRIMARY KEY (p_partkey)
);

INSERT INTO part (p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment)
SELECT p_partkey
     , p_name
     , p_mfgr
     , p_brand
     , p_type
     , p_size
     , p_container
     , p_retailprice
     , p_comment
FROM part_external;


CREATE TABLE supplier (
    s_suppkey   INTEGER        NOT NULL,
    s_name      CHAR(25)       NOT NULL,
    s_address   VARCHAR(40)    NOT NULL,
    s_nationkey INTEGER        NOT NULL,
    s_phone     CHAR(15)       NOT NULL,
    s_acctbal   DECIMAL(15, 2) NOT NULL,
    s_comment   VARCHAR(101)   NOT NULL,
    PRIMARY KEY (s_suppkey),
    FOREIGN KEY (s_nationkey) REFERENCES nation (n_nationkey)
);

INSERT INTO supplier (s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment)
SELECT s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment
FROM supplier_external;

CREATE TABLE partsupp (
    ps_partkey    INTEGER        NOT NULL,
    ps_suppkey    INTEGER        NOT NULL,
    ps_availqty   INTEGER        NOT NULL,
    ps_supplycost DECIMAL(15, 2) NOT NULL,
    ps_comment    VARCHAR(199)   NOT NULL,
    PRIMARY KEY (ps_partkey, ps_suppkey),
    FOREIGN KEY (ps_partkey) REFERENCES part (p_partkey),
    FOREIGN KEY (ps_suppkey) REFERENCES supplier (s_suppkey)
);

INSERT INTO partsupp (ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, ps_comment)
SELECT ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, ps_comment
FROM partsupp_external;

CREATE TABLE customer (
    c_custkey    INTEGER        NOT NULL,
    c_name       VARCHAR(25)    NOT NULL,
    c_address    VARCHAR(40)    NOT NULL,
    c_nationkey  INTEGER        NOT NULL,
    c_phone      CHAR(15)       NOT NULL,
    c_acctbal    DECIMAL(15, 2) NOT NULL,
    c_mktsegment CHAR(10)       NOT NULL,
    c_comment    VARCHAR(117)   NOT NULL,
    PRIMARY KEY (c_custkey),
    FOREIGN KEY (c_nationkey) REFERENCES nation (n_nationkey)
);

INSERT INTO customer (c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment)
SELECT c_custkey
     , c_name
     , c_address
     , c_nationkey
     , c_phone
     , c_acctbal
     , c_mktsegment
     , c_comment
FROM customer_external;

CREATE TABLE orders (
    o_orderkey      INTEGER        NOT NULL,
    o_custkey       INTEGER        NOT NULL,
    o_orderstatus   CHAR(1)        NOT NULL,
    o_totalprice    DECIMAL(15, 2) NOT NULL,
    o_orderdate     DATE           NOT NULL,
    o_orderpriority CHAR(15)       NOT NULL,
    o_clerk         CHAR(15)       NOT NULL,
    o_shippriority  INTEGER        NOT NULL,
    o_comment       VARCHAR(79)    NOT NULL,
    PRIMARY KEY (o_orderkey),
    FOREIGN KEY (o_custkey) REFERENCES customer (c_custkey)
);

INSERT INTO orders ( o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk
                   , o_shippriority, o_comment)
SELECT o_orderkey
     , o_custkey
     , o_orderstatus
     , o_totalprice
     , o_orderdate
     , o_orderpriority
     , o_clerk
     , o_shippriority
     , o_comment
FROM orders_external;

CREATE TABLE lineitem (
    l_orderkey      INTEGER        NOT NULL,
    l_partkey       INTEGER        NOT NULL,
    l_suppkey       INTEGER        NOT NULL,
    l_linenumber    INTEGER        NOT NULL,
    l_quantity      DECIMAL(15, 2) NOT NULL,
    l_extendedprice DECIMAL(15, 2) NOT NULL,
    l_discount      DECIMAL(15, 2) NOT NULL,
    l_tax           DECIMAL(15, 2) NOT NULL,
    l_returnflag    CHAR(1)        NOT NULL,
    l_linestatus    CHAR(1)        NOT NULL,
    l_shipdate      DATE           NOT NULL,
    l_commitdate    DATE           NOT NULL,
    l_receiptdate   DATE           NOT NULL,
    l_shipinstruct  CHAR(25)       NOT NULL,
    l_shipmode      CHAR(10)       NOT NULL,
    l_comment       VARCHAR(44)    NOT NULL,
    PRIMARY KEY (l_orderkey, l_linenumber),
    FOREIGN KEY (l_orderkey) REFERENCES orders (o_orderkey),
    FOREIGN KEY (l_partkey, l_suppkey) REFERENCES partsupp (ps_partkey, ps_suppkey)
);

INSERT INTO lineitem ( l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax
                     , l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode
                     , l_comment)
SELECT l_orderkey
     , l_partkey
     , l_suppkey
     , l_linenumber
     , l_quantity
     , l_extendedprice
     , l_discount
     , l_tax
     , l_returnflag
     , l_linestatus
     , l_shipdate
     , l_commitdate
     , l_receiptdate
     , l_shipinstruct
     , l_shipmode
     , l_comment
FROM lineitem_external;

COMMIT;
