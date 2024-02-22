import requests, json, time, pandas as pd
from API_keys import *
from Processing_functions import *

# --------------------------  API calls ---------------------------------#

def FedEx_API_authenticate():
    # Oauth API inputs
    url_auth =  'https://apis.fedex.com/oauth/token'
    input_auth = f'grant_type=client_credentials&client_id={client_prod_id}&client_secret={client_prod_secret}'
    headers_auth = {'Content-Type': "application/x-www-form-urlencoded"}
    # get response
    response_auth = requests.post(url_auth, data=input_auth, headers=headers_auth)
    try:
        access_token = json.loads(response_auth.text)['access_token']
        return access_token
    except KeyError:
        raise KeyError(f'Invalid response, response: {response_auth.text}')
    return "Key Error"

def call_addresses_FedEx_API(addresses_input, access_token):
    # Address API inputs
    url_add = "https://apis.fedex.com/address/v1/addresses/resolve"
    #print("access_token : ", access_token)

    headers_add = {
        'content-type': "application/json",
        'x-locale': "en_US",
        'authorization': f"Bearer {access_token}"}
    #get response
    response=requests.post(url_add, data=json.dumps(addresses_input), headers=headers_add)
    return response.text

def call_addresses_Loqate_API(addresses_input_list, Loqate_key):
    # Define the API endpoint
    url_add = 'https://ai.addressy.com//Cleansing/International/Batch/v1.00/json4.ws'

    # Define the headers
    headers_add = {
        'Content-Type': 'application/json'}
    
    # Define the payload
    addresses_input = {
            "Key": Loqate_key,
            "Geocode": True,
            "Options": {
                "Process": "verify",
                "ServerOptions": {
                    "OutputAddressFormat": "yes"
                }
            },
            "Addresses": addresses_input_list}
    
    #get response
    response=requests.post(url_add, data=json.dumps(addresses_input), headers=headers_add)

    return response


# --------------------------  Run batches ---------------------------------#
def run_batches(odl, batch_size, sleep,API_type):
    successful_batches = pd.DataFrame()
    failed_batches = pd.DataFrame()

    access_token = FedEx_API_authenticate()

    # Iterate over batches
    for i in range(0, len(odl), batch_size):
        addresses_to_test = odl.iloc[i:i + batch_size].reset_index(drop=True)

        if API_type == "FedEx":
            try:
            # Call the Address Validation API for the current batch
                addresses_input = create_FedEx_addresses_input(addresses_to_test)
                start_API_time = time.time()
                response = call_addresses_FedEx_API(addresses_input, access_token)
                end_API_time = time.time()
                resolved_addresses,transaction_id = create_FedEx_response_df(response)
                # Concatenate horizontally within the batch
                batch_result = pd.concat([addresses_to_test, resolved_addresses], axis=1)
                # Concatenate vertically with the successful_batches
                successful_batches = pd.concat([successful_batches, batch_result], axis=0, ignore_index=True)

                API_time = end_API_time - start_API_time
                # Print information and add a delay of 1 second
                print(f"Processed batch {i + 1}-{i + len(addresses_to_test)}; API call took {round(API_time, 2)} sec, Transaction ID {transaction_id}")
                time.sleep(sleep)
            except Exception as e:
                # Handle the exception (you may want to log or print the error)
                print(f"Error in processing batch {i + 1}-{i + len(addresses_to_test)}: {str(e)}")

                if 'JWT is expired' in str(e):
                    # Refresh the access token if it has expired
                    access_token = FedEx_API_authenticate()

                    # Retry the current batch after obtaining a new access token
                    addresses_input = create_FedEx_addresses_input(addresses_to_test)
                    start_API_time = time.time()
                    response = call_addresses_FedEx_API(addresses_input, access_token)
                    end_API_time = time.time()
                    resolved_addresses,transaction_id = create_FedEx_response_df(response)

                    # Concatenate horizontally within the batch
                    batch_result = pd.concat([addresses_to_test, resolved_addresses], axis=1)

                    # Concatenate vertically with the successful_batches
                    successful_batches = pd.concat([successful_batches, batch_result], axis=0, ignore_index=True)

                    API_time = end_API_time - start_API_time
                    # Print information and add a delay of 1 second
                    print(f"Processed batch {i + 1}-{i + len(addresses_to_test)}; API call took {round(API_time, 2)} sec, , Transaction ID {transaction_id}")

                else:
                    # Concatenate the failed batch to the failed_batches DataFrame
                    failed_batches = pd.concat([failed_batches, addresses_to_test], axis=0, ignore_index=True)
        else:
            addresses_input = create_Loqate_addresses_input(addresses_to_test)
            start_API_time = time.time()
            response = call_addresses_Loqate_API(addresses_input, access_token)
            end_API_time = time.time()

            if response.status_code == 200:
                # print(json.dumps(response.json(), indent=4))
                resolved_addresses = create_Loqate_response_df(response.json())
            else:
                print(f"Error in processing batch {i + 1}-{i + len(addresses_to_test)}: {str(response.status_code)}")
                failed_batches = pd.concat([failed_batches, addresses_to_test], axis=0, ignore_index=True)

    return successful_batches, failed_batches

def retry_failed(successful,failed,batch_size,sleep,API_type):
    prev_failed_size = len(failed)

    # Retry on failed batches:
    print("Retrying on Failed batches, for ",len(failed)," data")
    successful2,failed=run_batches(failed, batch_size, sleep,API_type)
    successful=pd.concat([successful,successful2],ignore_index=True)
    
    while len(failed) > 1 and len(failed) != prev_failed_size:
        prev_failed_size = len(failed)
        print("Retrying on Failed batches, for ",len(failed)," data")
        
        batch_size = max(1,batch_size//2)
        successful2,failed=run_batches(failed, batch_size, sleep,API_type)
        successful=pd.concat([successful,successful2],ignore_index=True)
        
    return successful,failed