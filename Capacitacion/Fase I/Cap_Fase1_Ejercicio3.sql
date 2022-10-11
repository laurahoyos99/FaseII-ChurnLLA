select distinct date_trunc('month',date(INTERACTION_START_TIME)) as month,interaction_purpose_descrip,count(distinct interaction_id) as interactions
from "db-stage-prod"."interactions_cwp"
where date_trunc('month',date(INTERACTION_START_TIME))=date('2022-08-01') -- filtro para especificar mes de agosto
group by 1,2 order by 1,2
