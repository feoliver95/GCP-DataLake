with nested_materials as (
select * from `visualizacao.bill_of_materials_unpivot`  as materials_unpivot
left join dotz.comp_boss as cb on ( cb.component_id  =  materials_unpivot.metricas.componente )
where component_id is not null
order by  component_id desc

)

SELECT 
    tube_assembly_id, 
   
    ARRAY_AGG(STRUCT( metricas.componente, metricas.quantidade, component_id, component_type_id, type, connection_type_id,
    outside_shape, base_type , height_over_tube, bolt_pattern_long, bolt_pattern_wide, groove, base_diameter, shoulder_diameter, unique_feature, orientation, weight)) AS attributes
from nested_materials
group by tube_assembly_id
order by tube_assembly_id