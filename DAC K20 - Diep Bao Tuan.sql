#Tasks:<\br> 
Query 01: calculate total visit, pageview, transaction for Jan, Feb and March 2017 (order by month)
Query 02: Bounce rate per traffic source in July 2017 (Bounce_rate = num_bounce/total_visit) (order by total_visit DESC)
Query 3: Revenue by traffic source by week, by month in June 2017
Query 04: Average number of pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017.
Query 05: Average number of transactions per user that made a purchase in July 2017
Query 06: Average amount of money spent per session. Only include purchaser data in July 2017
Query 07: Other products purchased by customers who purchased product "YouTube Men's Vintage Henley" in July 2017. Output should show product name and the quantity was ordered.
Query 08: Calculate cohort map from product view to addtocart to purchase in Jan, Feb and March 2017. For example, 100% product view then 40% add_to_cart and 10% purchase.
              Add_to_cart_rate = number product  add to cart/number product view. Purchase_rate = number product purchase/number product view. The output should be calculated in product level.

--Query 1
with process_data as(
       SELECT format_date('%Y%m',parse_date('%Y%m%d',date)) as Month, 
              sum(totals.visits)as NumberVisit, 
              sum(totals.pageviews)as PageViews,
              sum(totals.transactions)as transactions ,
       FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*` 
       --where _table_suffix between '01*' and '04*' 
       WHERE _TABLE_SUFFIX BETWEEN '0101' AND '0331'
       group by Month
       order by Month)
select *
from process_data;


--Query 2
with processed_data as 
  (SELECT distinct trafficSource.`source` as Source,
          sum(totals.visits)as Totals_visits,
          sum(totals.bounces) as Total_no_of_bounces,
          sum(totals.bounces)/sum(totals.visits)*100 as Bounce_rate
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
    group by SOURCE
    order by Totals_visits DESC)

  select * 
  from processed_data;


--Query 3
with by_week as(
      SELECT distinct --k cần distinct ở đây
            'Week' as Time_type,
            format_date('%Y%W',parse_date('%Y%m%d',date)) as Time, 
            trafficSource.`source` as Source,
            sum(productRevenue)/1000000 as Revenue
      FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
      UNNEST (hits) hits,
      UNNEST (hits.product) product
      where productRevenue is not NULL
      group by Time, Source
      order by Source, Time),
by_month as
       (SELECT
            'Month' as Time_type,
            format_date('%Y%m',parse_date('%Y%m%d',date)) as Time, 
            trafficSource.`source` as Source,
            sum(productRevenue)/1000000 as Revenue
      FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
      UNNEST (hits) hits,
      UNNEST (hits.product) product
      where productRevenue is not NULL
      group by Time, Source
      order by Source, Time)

select * 
from by_week
Union all
select* 
from by_month
order by Source;


--Query 4
with purchaser_Data as
  (SELECT format_date('%Y%m',parse_date('%Y%m%d',date)) as month,
        fullVisitorId,
        sum(totals.pageviews) as Totals_PageViews,
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`, 
  UNNEST (hits) hits,
  UNNEST (hits.product) product
  where _table_suffix between '0601'and '0731'   --cách e ghi and nó hơi kì, ngta thường ghi ở đầu câu hơn
  and   totals.transactions >= 1 
  and   product.productRevenue is not null
  group by fullVisitorId, month
  order by month, Totals_PageViews),

  none_Purchaser_Data as 
  (SELECT format_date('%Y%m',parse_date('%Y%m%d',date)) as month,
        fullVisitorId,
        sum(totals.pageviews) as Totals_PageViews,
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`, 
  UNNEST (hits) hits,
  UNNEST (hits.product) product
  where _table_suffix between '0601'and '0731' and 
        totals.transactions is null and 
        product.productRevenue is null
  group by fullVisitorId, month
  order by month, Totals_PageViews),

avg_pageviews_purchase as
  (select month,
        sum(Totals_PageViews)/count(distinct fullVisitorId ) as avg_pageviews_purchase
  from purchaser_Data
  group by month 
  order by month),

avg_pageviews_non_purchase as
  (select month,
        sum(Totals_PageViews)/count(distinct fullVisitorId ) as avg_pageviews_non_purchase
  from none_Purchaser_Data
  group by month 
  order by month)

select avg_pageviews_purchase.month,
         avg_pageviews_purchase.avg_pageviews_purchase,
         avg_pageviews_non_purchase.avg_pageviews_non_purchase
from avg_pageviews_purchase
inner join avg_pageviews_non_purchase
on avg_pageviews_purchase.month=avg_pageviews_non_purchase.month
order by month;


with 
purchaser_data as(
  select
      format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
      (sum(totals.pageviews)/count(distinct fullvisitorid)) as avg_pageviews_purchase,
  from `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
    ,unnest(hits) hits
    ,unnest(product) product
  where _table_suffix between '0601' and '0731'
  and totals.transactions>=1
  and product.productRevenue is not null
  group by month
),

non_purchaser_data as(
  select
      format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
      sum(totals.pageviews)/count(distinct fullvisitorid) as avg_pageviews_non_purchase,
  from `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
      ,unnest(hits) hits
    ,unnest(product) product
  where _table_suffix between '0601' and '0731'
  and totals.transactions is null
  and product.productRevenue is null
  group by month
)

select
    pd.*,
    avg_pageviews_non_purchase
from purchaser_data pd
left join non_purchaser_data using(month)
order by pd.month;



--Query 5
with totals_transactions_Data as
  (SELECT format_date('%Y%m',parse_date('%Y%m%d',date)) as month,
         fullVisitorId,
         sum(totals.transactions) as totals_transactions
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`, 
  UNNEST (hits) hits,
  UNNEST (hits.product) product
  where
        totals.transactions >= 1 and 
        product.productRevenue is not null
  group by  month, fullVisitorId)

select month,
       sum(totals_transactions)/count(distinct fullVisitorId) as Avg_total_transactions_per_user
from totals_transactions_Data
group by month;

select
    format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
    sum(totals.transactions)/count(distinct fullvisitorid) as Avg_total_transactions_per_user
from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
    ,unnest (hits) hits,
    unnest(product) product
where  totals.transactions>=1
and totals.totalTransactionRevenue is not null
and product.productRevenue is not null
group by month;


--Query 6
with get_Data as
  (SELECT format_date('%Y%m',parse_date('%Y%m%d',date)) as month,
         sum(product.productRevenue)/1000000 as productRevenue,
         sum(totals.visits) as totals_visits
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`, 
  UNNEST (hits) hits,
  UNNEST (hits.product) product
  where
        totals.transactions is not null and 
        product.productRevenue is not null
  group by  month)

select month,
       round(productRevenue/totals_visits,2) as avg_revenue_by_user_per_visit
from get_Data;

--correct
--cách ghi gọn hơn
select
    format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
    ((sum(product.productRevenue)/sum(totals.visits))/power(10,6)) as avg_revenue_by_user_per_visit
from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
  ,unnest(hits) hits
  ,unnest(product) product
where product.productRevenue is not null
group by month;


--Query 7
select  product.v2ProductName as other_purchased_products,
        sum(product.productQuantity) as quantity
        FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
        UNNEST (hits) hits,
        UNNEST (hits.product) product
        where   fullVisitorId in (select fullVisitorId                      
                                FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
                                UNNEST (hits) hits,
                                UNNEST (hits.product) product
                                where
                                product.v2ProductName="YouTube Men's Vintage Henley" and 
                                product.productRevenue is not null) 
        and --thay vì để chữ and ở trên, e để ở dưới đây, ngang hàng với chữ WHERE của main query, nó dễ nhìn hơn
                product.productRevenue is not null and
                product.v2ProductName != "YouTube Men's Vintage Henley"
        group by other_purchased_products
        order by quantity Desc;

--Query 8
with num_purchase as (
    select format_date('%Y%m',parse_date('%Y%m%d',date)) as month,
            count(hits.eCommerceAction.action_type) as num_purchase                 
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
    UNNEST (hits) hits,
    UNNEST (hits.product) product
    where _table_suffix between '01*' and '04*'and
          product.productRevenue is not null and
          hits.eCommerceAction.action_type = '6'
    group by month
    order by month), 

    num_addtocart as 
      (select format_date('%Y%m',parse_date('%Y%m%d',date)) as month,
              count(hits.eCommerceAction.action_type) as num_addtocart                
      FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
      UNNEST (hits) hits,
      UNNEST (hits.product) product
      where _table_suffix between '01*' and '04*'and
            hits.eCommerceAction.action_type = '3'
      group by month
      order by month),

    num_product_view as 
      (select format_date('%Y%m',parse_date('%Y%m%d',date)) as month,
              count(hits.eCommerceAction.action_type) as num_product_view                
      FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
      UNNEST (hits) hits,
      UNNEST (hits.product) product
      where _table_suffix between '01*' and '04*'and
            hits.eCommerceAction.action_type = '2'
      group by month
      order by month),
    
  join_DT as 
  (
    select num_product_view.month,
           num_product_view.num_product_view,
           num_addtocart.num_addtocart,
           num_purchase.num_purchase,
    from num_product_view
    left join num_addtocart 
    on num_product_view.month=num_addtocart.month
    left join num_purchase
    on num_product_view.month=num_purchase.month
  )
select *,
      round((num_addtocart/num_product_view)*100,2) as add_to_cart_rate,
      round((num_purchase/num_product_view)*100,2) as purchase_rate
from join_DT
order by month;

--bài yêu cầu tính số sản phầm, mình nên count productName hay productSKU thì sẽ hợp lý hơn là count action_type
--Cách 1:dùng CTE
with
product_view as(
SELECT
  format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
  count(product.productSKU) as num_product_view
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
, UNNEST(hits) AS hits
, UNNEST(hits.product) as product
WHERE _TABLE_SUFFIX BETWEEN '20170101' AND '20170331'
AND hits.eCommerceAction.action_type = '2'
GROUP BY 1
),

add_to_cart as(
SELECT
  format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
  count(product.productSKU) as num_addtocart
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
, UNNEST(hits) AS hits
, UNNEST(hits.product) as product
WHERE _TABLE_SUFFIX BETWEEN '20170101' AND '20170331'
AND hits.eCommerceAction.action_type = '3'
GROUP BY 1
),

purchase as(
SELECT
  format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
  count(product.productSKU) as num_purchase
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
, UNNEST(hits) AS hits
, UNNEST(hits.product) as product
WHERE _TABLE_SUFFIX BETWEEN '20170101' AND '20170331'
AND hits.eCommerceAction.action_type = '6'
and product.productRevenue is not null   --phải thêm điều kiện này để đảm bảo có revenue
group by 1
)

select
    pv.*,
    num_addtocart,
    num_purchase,
    round(num_addtocart*100/num_product_view,2) as add_to_cart_rate,
    round(num_purchase*100/num_product_view,2) as purchase_rate
from product_view pv
left join add_to_cart a on pv.month = a.month
left join purchase p on pv.month = p.month
order by pv.month;


with product_data as(
select
    format_date('%Y%m', parse_date('%Y%m%d',date)) as month,
    count(CASE WHEN eCommerceAction.action_type = '2' THEN product.v2ProductName END) as num_product_view,
    count(CASE WHEN eCommerceAction.action_type = '3' THEN product.v2ProductName END) as num_add_to_cart,
    count(CASE WHEN eCommerceAction.action_type = '6' and product.productRevenue is not null THEN product.v2ProductName END) as num_purchase
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
,UNNEST(hits) as hits
,UNNEST (hits.product) as product
where _table_suffix between '20170101' and '20170331'
and eCommerceAction.action_type in ('2','3','6')
group by month
order by month
)

select
    *,
    round(num_add_to_cart/num_product_view * 100, 2) as add_to_cart_rate,
    round(num_purchase/num_product_view * 100, 2) as purchase_rate
from product_data;


                                                 



