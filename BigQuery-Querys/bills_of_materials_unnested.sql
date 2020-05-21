with bills as (
SELECT tube_assembly_id , 
 [
  
  STRUCT(component_id_1 as componente, quantity_1 as quantidade),
    STRUCT(component_id_2  as componente, quantity_2 as quantidade),
    STRUCT(component_id_3  as componente, quantity_3 as quantidade) ,
  STRUCT(component_id_4  as componente, quantity_4 as quantidade),
   STRUCT(component_id_5  as componente, quantity_5 as quantidade),
    STRUCT(component_id_6  as componente, quantity_6 as quantidade),
    STRUCT(component_id_7  as componente, quantity_7 as quantidade),
   STRUCT(component_id_8  as componente, quantity_8 as quantidade)
  
  ] as componentes
  from `dotz_views.bill_of_materials_view`  
  )
  
SELECT
  tube_assembly_id ,
  metricas

FROM
  bills
CROSS JOIN
  UNNEST(bills.componentes) AS metricas

  
