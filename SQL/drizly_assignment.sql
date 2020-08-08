-- Author: Jeff Willingham
-- Date: 2020-08-07
-- Purpose: Drizly Assessment
-- Software: Ver 8.0.21 for macos10.15 on x86_64 (MySQL Community Server - GPL)

set global local_infile = on;

drop database if exists drizly;

create database drizly;

use drizly;

drop table if exists drizly.store_orders;

create table drizly.store_orders 
(
  store_order_id   int,
  order_id         int,
  store_id         int,
  date_delivered   date,
  user_id          int
);

load data local infile '/Users/willingham/Downloads/store_orders.csv' into table drizly.store_orders fields terminated by ',' lines terminated by '\n' ignore 1 lines;

drop table if exists drizly.store_order_items;

create table drizly.store_order_items 
(
  store_order_id       int,
  date_delivered       date,
  user_id              int,
  unit_price           float,
  quantity             int,
  master_item_id       int,
  top_level_category   varchar(255)
);

load data local infile '/Users/willingham/Downloads/store_order_items.csv' into table drizly.store_order_items fields terminated by ',' lines terminated by '\n' ignore 1 lines;

-- 1. How many orders were placed in the first 10 days of July?
-- orders_placed = 198233
select count(store_order_id) orders_placed
from drizly.store_orders
where date_delivered between cast('2020-07-01' as date) and cast('2020-07-10' as date);

-- 2. How many new stores were added on or after July 15th?
-- new_stores = 116
select count(distinct store_id) new_stores
from drizly.store_orders
where store_id not in (select distinct store_id
                       from drizly.store_orders
                       where date_delivered < cast('2020-07-15' as date));

-- 3. How many users placed their third wine order (only consider orders with at least one wine item) on or after July 15th?
-- users = 8723
select count(tb1.user_id) users
from (select user_id
      from drizly.store_order_items
      where date_delivered < cast('2020-07-15' as date)
      and   top_level_category = 'Wine'
      group by user_id
      having count(distinct store_order_id) < 3) tb1
  join (select user_id
        from drizly.store_order_items
        where top_level_category = 'Wine'
        group by user_id
        having count(distinct store_order_id) >= 3) tb2 on tb1.user_id = tb2.user_id;

-- 4. What was the GMV of those orders (total of all items, regardless of top level category)?
-- gmv = 2330737.15
select sum(unit_price*quantity) gmv
from drizly.store_order_items
where user_id in (select tb1.user_id
                  from (select user_id
                        from drizly.store_order_items
                        where date_delivered < cast('2020-07-15' as date)
                        and   top_level_category = 'Wine'
                        group by user_id
                        having count(distinct store_order_id) < 3) tb1
                    join (select user_id
                          from drizly.store_order_items
                          where top_level_category = 'Wine'
                          group by user_id
                          having count(distinct store_order_id) >= 3) tb2 on tb1.user_id = tb2.user_id);

-- 5. In a single SQL query, return the total number of items purchased for ​every store​ and every day​ in this data set (no need to include output).
select tb1.store_id,
       tb2.date_delivered,
       sum(quantity) items_purchased
from (select * from drizly.store_orders) tb1
  join (select * from drizly.store_order_items) tb2 on tb1.store_order_id = tb2.store_order_id
group by tb1.store_id,
         tb2.date_delivered;

-- 6. (​Bonus question unrelated to the data set​): For each visit to the site we record down the list of stores that are able to
-- deliver to the user in the format stores-{pipe-delimited store IDs} e.g. stores-35|120|530, stores-47,​ or ​stores-427|3619|12|36|490.
-- Please write a regular expression pattern to match valid data entries with at least one store (store IDs are always pure integers).
-- Example invalid entries:
-- ● stores-24|
-- ● stores-25|29||58
-- ● stores-24s|z29
-- ● storiees-25|29|58
-- ● the-stores-25|29|58
-- ● stores-25|29|58-end
-- ^stores[-][0-9]+(\|[0-9]+)*

