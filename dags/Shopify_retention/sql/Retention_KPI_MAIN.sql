CREATE OR REPLACE TABLE `shopify-pubsub-project.Retention_Cohort.Retention_KPI_MAIN_Final` AS
SELECT 
  r_k.*, 
  e_o.* EXCEPT (year_month), 
  me_sp.* EXCEPT (month_start), 
  or_c.* EXCEPT (year_month,RC), 
  m0_c.M0_cont,m0_c.M0_Contri_Per, 
  dis.* EXCEPT(year_month),
  crm.* EXCEPT (year_month)
FROM 
  `shopify-pubsub-project.Retention_Cohort.Retention_KPI` r_k 
INNER JOIN 
  `shopify-pubsub-project.Retention_Cohort.Retention_KPI_Exact_Order_Count` e_o 
  ON r_k.year_month = e_o.year_month
INNER JOIN 
  `shopify-pubsub-project.Retention_Cohort.Retention_KPI_Meta_spend_pp` me_sp
  ON r_k.year_month = me_sp.month_start 
INNER JOIN 
  `shopify-pubsub-project.Retention_Cohort.Retenton_KPI_Order_Count` or_c 
  ON r_k.year_month = or_c.year_month  
INNER JOIN 
  `shopify-pubsub-project.Retention_Cohort.Retention_KPI_M0Cont`m0_c  
  ON r_k.year_month = m0_c.year_month 
INNER JOIN 
 `shopify-pubsub-project.Retention_Cohort.Retention_KPI_bogo_dis` dis 
  ON r_k.year_month = dis.year_month
INNER JOIN 
  `shopify-pubsub-project.Retention_Cohort.Retention_KPI_crm_metrics` crm 
  ON r_k.year_month = crm.year_month

