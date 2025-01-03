# GITHUB DIRECTORY BUILDER(ON LOCAL SYS.) FOR DATA ENGINEERING PROJECTS

The python script "Directory_Builder.ipynb" helps you create the desired Github Directory for the Data Engineering projects. The code reads all the table names from a given project name, dataset name and then proceeds to create directory that resembles the Github directory path. The standard directory format looks like below:


		BI __
		     |
		     |__ dags __
				|
				|__ {Data_Engg_folder}__
							|
					 		|__ dag __ dummy_dag.py (empty python file)
							|	
					 		|__ sql __
								  |	
					 			  |__ data master __ [dummy_master_create.sql, dummy_master_append.sql]
								  |
								  |__ Data Sanity Check __ dummy_sanity_check.sql
								  |
								  |__ {Data_Engg_Folder}_to_bq __
												 |______ Folder_1 _____ [Folder_1_create.sql, Folder_1_append.sql]
												 |______ Folder_2 _____ [Folder_2_create.sql, Folder_2_append.sql]
												 |______ Folder_3 _____ [Folder_3_create.sql, Folder_3_append.sql]
												 .
												 .
												 .
												 |______ Folder_n _____ [Folder_n_create.sql, Folder_n_append.sql]
 
## Follow below steps in order to run this script successfully:

1. Right click inside the folder containing this readme file and click on "Open in terminal" or "Open in Powershell" (whichever is visible after right click)
2. Copy paste the below code and press enter to install required libraries:
	pip install google-cloud-bigquery pandas db-dtypes
3. Navigate to the folder containing this readme file, copy the full location of the "JSON_KEY" file by
	> FOR WINDOWS 11: right click on JSON_KEY and select copy as path
				OR
	> Click on the JSON_KEY file, locate the "Copy path" button visible below cut button within the folder.
4. Re-open the terminal or Powershell that you opened in step 1, type jupyter notebook to open Jupyter Notebook in browser.
5. Now you shall be able to see the "Directory Builder.ipynb" file. Double click to open the file.
6. After opening the "Directory Builder.ipynb" file, make the necessary changes to the code:
	> line 23: key_file_path = "path of the JSON_KEY" (copied in step 3)
	> line 32: fb_ads_path = os.path.join(dags_path, "fb_ads_warehouse") replace "fb_ads_path" and "fb_ads_warehouse" with suitable name
	> line 42: big_query_folder = "fb_ads_to_bq"    # Give the name of the folder that will contain the create and 	append sql codes
	> line 66: project_id = "your-project-id"
	> line 67: dataset_id = "your-dataset-id"
