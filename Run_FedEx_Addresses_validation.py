import pandas as pd
from API_calls_functions import *
from API_Processing_functions import *
from Analysis_functions import *

# Inputs
paths = [#'data-W05/2024-W05 HF Saturday PDL_240123000000_STANDARD.csv',
         #'data-W05/2024-W05 HF Sunday PDL_240124000000_STANDARD.csv', 
         #'data-W05/2024-W05 HF Monday PDL_240125000000_STANDARD.csv'#,
         #'data-W05/2024-W05 HF Tuesday PDL_240126000000_STANDARD.csv',
         #'data-W05/2024-W05 HF Wednesday PDL_240127000000_STANDARD.csv',
         #'data-W05/2024-W05 HF Thursday PDL_240128000000_STANDARD.csv',
         "data-W05/2024-W05 HF Friday PDL_240129000000_STANDARD.csv"
         ]


batch_size=100 #Max 100

sleep=1 #Pause between 2 API calls, in seconds

save = True #If we want to save results in output paths



for pdl_path in paths:
    print("\n------------------------------------------------------------------------ ")
    print("Running PDL ",pdl_path)
    print("------------------------------------------------------------------------ \n")
    # Defining Output paths
    successful_output_path=pdl_path[:-25]+'post-FedExAPI-successful.csv'
    failed_output_path=pdl_path[:-25]+'post-FedExAPI-failed.csv'

    # Read PDL data
    pdl=pdl_read_and_process(pdl_path)
    print("Length PDL : ",len(pdl))

    # Run batches of API calls
    successful,failed=run_batches(pdl, batch_size, sleep,"FedEx")
    if len(failed)>0:
        successful,failed=retry_failed(successful,failed,batch_size//2,sleep,"FedEx")

    if save:
        successful.to_csv(successful_output_path, index=False)
        if len(failed)>0:
            failed.to_csv(failed_output_path, index=False)

    print("\n-----Stats-----")
    print("Nb addresses API failed : ",len(failed))
    print_save_stats(successful,pdl_path,save)


    # Get postcode changes
    #successful_all[successful_all['postcode'].astype(int) != successful_all['postcode_R'].astype(int)].to_csv("data-W05/postcode_changes_FedEx.csv", index=False)