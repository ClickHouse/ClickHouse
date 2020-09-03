-- NOT SUPPORTED
SELECT *
FROM (
      SELECT *
      FROM cdp.recommendation_metadata_prod
               GLOBAL ANY
               LEFT JOIN
           (SELECT recommend_id       AS uuid,
                   sum(sends)         AS sends,
                   sum(opens)         AS opens,
                   sum(clicks)        AS clicks,
                   sum(unique_clicks) AS unique_clicks,
                   sum(unique_opens)  AS unique_opens,
                   sum(orders)        AS orders,
                   sum(revenue)       AS revenue,
                   sum(activations)   AS activations,
                   sum(revenue_gmv)   AS revenue_gmv,
                   sum(revenue_cmv)   AS revenue_cmv,
                   sum(revenue_nmv)   AS revenue_nmv,
                   sum(orders_nmv)    AS orders_nmv,
                   sum(orders_cmv)    AS orders_cmv
            FROM (SELECT date_key,
                         recommend_id,
                         sends,
                         opens,
                         clicks,
                         unique_clicks,
                         unique_opens,
                         orders,
                         transaction_revenue AS revenue,
                         activations,
                         transaction_gmv     AS revenue_gmv,
                         cmv                 AS revenue_cmv,
                         nmv                 AS revenue_nmv,
                         net_orders          AS orders_nmv,
                         confirmed_orders    AS orders_cmv
                  FROM cdp.view_campaign_performance
                  WHERE date_key BETWEEN '2020-05-21' AND '2020-05-27'
                    AND isNotNull(recommend_id)
                    AND notEmpty(recommend_id)

                  UNION ALL

                  SELECT date_key,
                         recommend_id,
                         sends,
                         opens,
                         clicks,
                         unique_clicks,
                         unique_opens,
                         total_orders  AS orders,
                         total_revenue AS revenue,
                         activations,
                         0.0           AS revenue_gmv,
                         0.0           AS revenue_cmv,
                         0.0           AS revenue_nmv,
                         0             AS orders_nmv,
                         0             AS orders_cmv
                  FROM cdp.realtime_campaign_performance
                  WHERE date_key BETWEEN '2020-05-21' AND '2020-05-27'
                    AND isNotNull(recommend_id)
                    AND notEmpty(recommend_id)
                     )
            GROUP BY uuid)
           USING (uuid)
      WHERE 1=1
      ORDER BY event_time DESC
      LIMIT 1 BY uuid)
ORDER BY created_at DESC
LIMIT 10 OFFSET 0;


