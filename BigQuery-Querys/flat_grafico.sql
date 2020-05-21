SELECT
  pq.tube_assembly_id price_id,
  supplier,
  quote_date,
  annual_usage,
  min_order_quantity,
  bracket_pricing,
  quantity,
  cost,
  nm.*
FROM
  `dotz_views.price_quote_view` AS pq
LEFT JOIN
  `visualizacao.nested_materials` AS nm
ON
  (pq.tube_assembly_id = nm.tube_assembly_id )
ORDER BY
  nm.tube_assembly_id DESC