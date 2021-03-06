

CREATE TABLE "dataWareHouse"."DIM_IMP_IDENTIFICATION" (
	id int GENERATED ALWAYS AS IDENTITY primary KEY,
    "uid" text,
	summary_EudraCT_Number text NULL,
	imp_role text NULL,	
	imp_marketing_authorisation text NULL,
	imp_Trade_name text NULL,
	imp_marketing_authorisation_holder text NULL,
	imp_orphan_drug text NULL,
	imp_orphan_drug_number text NULL,
	imp_productName text NULL,
	imp_productCode text NULL,
	--imp_pharmaceutical_form text NULL,
	--imp_paediatric_formulation text NULL,
	--imp_admin_routes text NULL,
	--"imp_INN" text NULL,
	--imp_casnumber text NULL,
	--imp_substance_code text NULL,
	--"imp_concentrationType" text NULL,
	--"imp_concentrationNumber" text NULL,
	--"imp_concentrationUnit" text NULL,
	--imp_chemical_origin text NULL,
	--imp_biotechnological_origin text NULL,
	--"imp_ATIMP" text NULL,
	--imp_somatic_cell_therapy text NULL,
	--imp_gene_therapy text NULL,
	--imp_tissue_engineered text NULL,
	--"imp_Combination_ATIMP" text NULL,
	--"imp_CAT_classification" text NULL,
	--"imp_advanced_Therapy" text NULL,
	--imp_placebo_used text NULL,
	file_Name text NULL,
	valid_from timestamp,
	valid_to timestamp default null
);
