/* Note - the following presently works in PostgreSQL 14 */

DROP TABLE IF EXISTS aggregation_results_bitmap_temp;

/* Duration: 53s */
CREATE TEMPORARY TABLE aggregation_results_bitmap_temp
AS
WITH starting_dataset AS (
    SELECT customer_region.r_regionkey AS customer_region_key
         , customer_region.r_name AS customer_region_name
         --
         , customer_nation.n_nationkey AS customer_nation_key
         , customer_nation.n_name AS customer_nation_name
         --
         , customer.c_custkey AS customer_key
         /* Setting bitmap size to NDV for the entire dataset for efficiency */
         , SET_BIT('0'::bit(19864), (DENSE_RANK() OVER (ORDER BY customer.c_custkey ASC) - 1)::int, 1) AS customer_bitmap
         --
         , part.p_mfgr AS part_manufacturer
         , part.p_brand AS part_brand
         , part.p_partkey AS part_key
         , part.p_name AS part_name
         --
         , orders.o_orderkey AS order_key
         , EXTRACT (YEAR FROM orders.o_orderdate) AS order_year
         , EXTRACT (YEAR FROM orders.o_orderdate) || '-Q' || EXTRACT (QUARTER FROM orders.o_orderdate) AS order_quarter
         , EXTRACT (YEAR FROM orders.o_orderdate) || '-' || EXTRACT (MONTH FROM orders.o_orderdate) AS order_month
         , orders.o_orderdate AS order_date
         , orders.o_orderstatus AS order_status
         --
         , lineitem.l_linenumber AS line_number
         , lineitem.l_quantity AS quantity
         , lineitem.l_extendedprice AS extended_price
         , lineitem.l_discount AS discount
         , lineitem.l_tax AS tax
         , lineitem.l_linestatus AS line_status
      FROM orders
        JOIN lineitem
        ON orders.o_orderkey = lineitem.l_orderkey
        JOIN customer
        ON orders.o_custkey = customer.c_custkey
        JOIN nation AS customer_nation
        ON customer.c_nationkey = customer_nation.n_nationkey
        JOIN region AS customer_region
        ON customer_nation.n_regionkey = customer_region.r_regionkey
        JOIN part
        ON lineitem.l_partkey = part.p_partkey
/* Change filter conditions here */
    WHERE customer_region.r_name = 'AMERICA'
      AND part.p_mfgr = 'Manufacturer#1'
)
/* We perform a "squash" aggregation here to reduce the rows going into the ROLLUP aggregation in the next step.
   This is only possible because we have a bitmap...
 */
, first_aggregation AS (
SELECT customer_region_name
     , customer_nation_name
     , part_manufacturer
     , part_brand
     , order_year
     , order_quarter
     , order_month
     --
     , BIT_OR (customer_bitmap) AS customer_bitmap
     , SUM (quantity) AS sum_quantity
     , SUM (extended_price) AS sum_extended_price
     , SUM (discount) AS sum_discount
     , SUM (tax) AS sum_tax
     , COUNT (*) AS count_star
  FROM starting_dataset
GROUP BY customer_region_name
     , customer_nation_name
     , part_manufacturer
     , part_brand
     , order_year
     , order_quarter
     , order_month
)
SELECT CASE GROUPING (customer_region_name)
          WHEN 0 THEN customer_region_name
          WHEN 1 THEN '(All)'
       END AS customer_region
     --
     , CASE GROUPING (customer_nation_name)
          WHEN 0 THEN customer_nation_name
          WHEN 1 THEN '(All)'
       END AS customer_nation
     --
     , CASE GROUPING (part_manufacturer)
          WHEN 0 THEN part_manufacturer
          WHEN 1 THEN '(All)'
       END AS part_manufacturer
     , CASE GROUPING (part_brand)
          WHEN 0 THEN part_brand
          WHEN 1 THEN '(All)'
       END AS part_brand
     --
     , CASE GROUPING (order_year::VARCHAR)
          WHEN 0 THEN order_year::VARCHAR
          WHEN 1 THEN '(All)'
       END AS order_year
     , CASE GROUPING (order_quarter::VARCHAR)
          WHEN 0 THEN order_quarter::VARCHAR
          WHEN 1 THEN '(All)'
       END AS order_quarter
     , CASE GROUPING (order_month::VARCHAR)
          WHEN 0 THEN order_month::VARCHAR
          WHEN 1 THEN '(All)'
       END AS order_month
     --
     , BIT_COUNT (BIT_OR (customer_bitmap)) AS customer_distinct_count
     , SUM (sum_quantity) AS sum_quantity
     , SUM (sum_extended_price) AS sum_extended_price
     , SUM (sum_discount) AS sum_discount
     , SUM (sum_tax) AS sum_tax
     , SUM (count_star) AS count_star
  FROM first_aggregation
GROUP BY
    ROLLUP (customer_region_name
          , customer_nation_name
           )
  , ROLLUP (part_manufacturer
          , part_brand
           )
  , ROLLUP (order_year::VARCHAR
          , order_quarter::VARCHAR
          , order_month::VARCHAR
           )
;

DROP TABLE IF EXISTS aggregation_results_temp;

/* Duration: 60s */
CREATE TEMPORARY TABLE aggregation_results_temp
AS
WITH starting_dataset AS (
    SELECT customer_region.r_regionkey AS customer_region_key
         , customer_region.r_name AS customer_region_name
         --
         , customer_nation.n_nationkey AS customer_nation_key
         , customer_nation.n_name AS customer_nation_name
         --
         , customer.c_custkey AS customer_key
         --
         , part.p_mfgr AS part_manufacturer
         , part.p_brand AS part_brand
         , part.p_partkey AS part_key
         , part.p_name AS part_name
         --
         , orders.o_orderkey AS order_key
         , EXTRACT (YEAR FROM orders.o_orderdate) AS order_year
         , EXTRACT (YEAR FROM orders.o_orderdate) || '-Q' || EXTRACT (QUARTER FROM orders.o_orderdate) AS order_quarter
         , EXTRACT (YEAR FROM orders.o_orderdate) || '-' || EXTRACT (MONTH FROM orders.o_orderdate) AS order_month
         , orders.o_orderdate AS order_date
         , orders.o_orderstatus AS order_status
         --
         , lineitem.l_linenumber AS line_number
         , lineitem.l_quantity AS quantity
         , lineitem.l_extendedprice AS extended_price
         , lineitem.l_discount AS discount
         , lineitem.l_tax AS tax
         , lineitem.l_linestatus AS line_status
      FROM orders
        JOIN lineitem
        ON orders.o_orderkey = lineitem.l_orderkey
        JOIN customer
        ON orders.o_custkey = customer.c_custkey
        JOIN nation AS customer_nation
        ON customer.c_nationkey = customer_nation.n_nationkey
        JOIN region AS customer_region
        ON customer_nation.n_regionkey = customer_region.r_regionkey
        JOIN part
        ON lineitem.l_partkey = part.p_partkey
/* Change filter conditions here */
    WHERE customer_region.r_name = 'AMERICA'
      AND part.p_mfgr = 'Manufacturer#1'
)
SELECT CASE GROUPING (customer_region_name)
          WHEN 0 THEN customer_region_name
          WHEN 1 THEN '(All)'
       END AS customer_region
     --
     , CASE GROUPING (customer_nation_name)
          WHEN 0 THEN customer_nation_name
          WHEN 1 THEN '(All)'
       END AS customer_nation
     --
     , CASE GROUPING (part_manufacturer)
          WHEN 0 THEN part_manufacturer
          WHEN 1 THEN '(All)'
       END AS part_manufacturer
     , CASE GROUPING (part_brand)
          WHEN 0 THEN part_brand
          WHEN 1 THEN '(All)'
       END AS part_brand
     --
     , CASE GROUPING (order_year::VARCHAR)
          WHEN 0 THEN order_year::VARCHAR
          WHEN 1 THEN '(All)'
       END AS order_year
     , CASE GROUPING (order_quarter::VARCHAR)
          WHEN 0 THEN order_quarter::VARCHAR
          WHEN 1 THEN '(All)'
       END AS order_quarter
     , CASE GROUPING (order_month::VARCHAR)
          WHEN 0 THEN order_month::VARCHAR
          WHEN 1 THEN '(All)'
       END AS order_month
     --
     , COUNT (DISTINCT customer_key) AS customer_distinct_count
     , SUM (quantity) AS sum_quantity
     , SUM (extended_price) AS sum_extended_price
     , SUM (discount) AS sum_discount
     , SUM (tax) AS sum_tax
     , COUNT (*) AS count_star
  FROM starting_dataset
GROUP BY
    ROLLUP (customer_region_name
          , customer_nation_name
           )
  , ROLLUP (part_manufacturer
          , part_brand
           )
  , ROLLUP (order_year::VARCHAR
          , order_quarter::VARCHAR
          , order_month::VARCHAR
           )
;

/* QA */
select *
 from aggregation_results_bitmap_temp
where customer_region = '(All)'
 and customer_nation = '(All)'
and part_manufacturer = '(All)'
and part_brand = '(All)'
and order_quarter = '(All)'
union all
select *
 from aggregation_results_temp
where customer_region = '(All)'
 and customer_nation = '(All)'
and part_manufacturer = '(All)'
and part_brand = '(All)'
and order_quarter = '(All)'
;
