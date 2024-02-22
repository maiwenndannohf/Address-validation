import json, re, pandas as pd
from unidecode import unidecode
from API_keys import *

# --------------------------  Pre-processing ---------------------------------#

def check_apt_format(word):
    # Regular expression to check if a word contains max 1 letter but can contain numbers and symbols
    pattern = re.compile(r'^[^a-zA-Z]*[a-zA-Z]?[^a-zA-Z]*$')
    return bool(pattern.match(word))

def clean_alphanumeric(string):
    # Regular expression to match non-alphanumeric characters at the beginning and end of the string
    pattern = re.compile(r'^[^a-zA-Z0-9#]*|[^a-zA-Z0-9#]*$')
    
    # Remove matched non-alphanumeric characters from the beginning and end of the string
    result = pattern.sub('', string)
    
    return result

def cleaning_addresses(row):
    # Use unicode to correct special characters
    street_1_clean = unidecode(str(row['street'])) if pd.notnull(row['street']) else str(row['street'])
    street_2_clean = unidecode(str(row['street_2'])) if pd.notnull(row['street_2']) else str(row['street_2'])
    city_clean = unidecode(str(row['city'])) if pd.notnull(row['city']) else str(row['city'])

    unicode_flag = (street_1_clean != row['street']) | (street_2_clean != row['street_2']) | (city_clean != row['city'])

    city_flag = row['city'] is not None and len(str(row['city'])) > 25
    street_1_flag = row['street'] is not None and len(str(row['street'])) > 40
    street_2_flag = row['street_2'] is not None and len(str(row['street_2'])) > 35
    
    if city_flag:
        city_clean = city_clean[:25]

    if street_1_flag:
        street_1_clean = street_1_clean[:40]
    
    if street_2_flag:
        street_w = street_1_clean.split()
        street_2_w = street_2_clean.split()
        city_w = city_clean.split()
        postcode_w = str(row['postcode']).split()
        state_w = str(row['state']).split()

        street_2_clean1 = ' '.join(clean_alphanumeric(word) for word in street_2_w[:4] if word not in street_w and word not in city_w and word not in postcode_w and word not in state_w)
        street_2_clean_w = street_2_clean1.split()

        if street_2_clean_w:
            for i in range(len(street_2_clean_w) - 1):
                if ('apt' in street_2_clean_w[i].lower() or 'apartment' in street_2_clean_w[i].lower() or 'unit' in street_2_clean_w[i].lower()) and check_apt_format(street_2_clean_w[i+1]):
                    street_2_clean = ' '.join(street_2_clean_w[i:i+2])
                    break
                elif street_2_clean_w[i].startswith('#') and check_apt_format(street_2_clean_w[i]):
                    street_2_clean = str(street_2_clean_w[i])
                    break
            else:
                street_2_clean = ''
        else:
            street_2_clean = ''
    else:
        street_2_clean = str(row['street_2'])
    
    return city_flag, city_clean, street_1_flag, street_1_clean,street_2_flag, street_2_clean, unicode_flag

def pdl_read_and_process(pdl_path):
    if pdl_path.lower().endswith(".csv"):
        pdl=pd.read_csv(pdl_path,dtype = 'str')[['du_id','customer_id','street','street_2','postcode','city','state']].drop_duplicates() # some customers have multiple du_id - we keep duplicates for now
    else:
        pdl=pd.read_excel(pdl_path,dtype = 'str')[['du_id','customer_id','street','street_2','postcode','city','state']].drop_duplicates() # some customers have multiple du_id - we keep duplicates for now
    pdl[['city_flag', 'city_clean','street_1_flag', 'street_1_clean','street_2_flag', 'street_2_clean', 'unicode_flag']] = pdl.apply(cleaning_addresses, axis=1, result_type='expand')
    return pdl


def create_FedEx_addresses_input(addresses_to_test):
    addresses_dict = []
    for index, row in addresses_to_test.iterrows():
        address={
            "streetLines": [str(row['street_1_clean']), str(row['street_2_clean'])],
            "stateOrProvinceCode": str(row['state']),
            "city": row['city_clean'],
            "postalCode": str(row['postcode']),
            "countryCode": "US"
        }
        addresses_dict.append({"address":address})
    addresses_input = {"addressesToValidate": addresses_dict}
    return addresses_input

def create_Loqate_addresses_input(addresses_to_test):
    addresses_list = []

    for index, row in addresses_to_test.iterrows():
        address = {
            "Address1": str(row['street_1_clean']),
            "Address2": str(row['street_2_clean']),
            "Locality": row['city_clean'],
            "PostalCode": str(row['postcode']),
            "Country": "US"
        }
        addresses_list.append(address)

    return addresses_list


# ------------------------------------------ Post-processing ---------------------------

def create_FedEx_response_df(response):
    transaction_id=""
    try:
        resolved_addresses_json=json.loads(response)['output']['resolvedAddresses']
        transaction_id=str(json.loads(response)['transactionId'])

    except KeyError:
        raise KeyError(f'Invalid response, response: {response}')
    features = []

    for address in resolved_addresses_json:
        streetline = address.get("streetLinesToken", [])
        features.append({
            "street_R": str(streetline[0]),
            "street_2_R": str(streetline[1]) if len(streetline) > 1 else "",
            "postcode_R": str(address.get("postalCode", "")[0:5]),
            "city_R": str(address.get("city")),
            "state_R": str(address.get("stateOrProvinceCode")),
            "customerMessages": str(address.get("customerMessages", [])),
            "SuiteRequiredButMissing": str(address["attributes"].get("SuiteRequiredButMissing")),
            "InvalidSuiteNumber": str(address["attributes"].get("InvalidSuiteNumber")),
            "Matched": str(address["attributes"].get("Matched")),
            "DPV": str(address["attributes"].get("DPV","")),
            "DataVintage": str(address["attributes"].get("DataVintage","")),
            "Resolved": str(address["attributes"].get("Resolved","")),
            "AddressType": str(address["attributes"].get("AddressType","")),
            "AddressPrecision": str(address["attributes"].get("AddressPrecision","")),
            "Interpolated": str(address["attributes"].get("Interpolated",""))
        })

    # Create DataFrame
    resolved_addresses = pd.DataFrame(features)
    return resolved_addresses,transaction_id

def create_Loqate_response_df(response_json):

    ####
        ### TO DO
    ####
    return
