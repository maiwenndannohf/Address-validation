import pandas as pd, string
from nltk.tokenize import word_tokenize #nltk.download('punkt')
# --------------------------  STATS ---------------------------------#

def print_save_stats(result_df, path, save):
    output_lines = []

    total_addresses = len(result_df)
    output_lines.append("Nb addresses API checked: {}".format(total_addresses))

    DPV_valid_count = (result_df['DPV'].apply(lambda x: str(x)).str.lower() == "true").sum()
    DPV_valid_percentage = (DPV_valid_count / total_addresses) * 100
    output_lines.append("Nb DPV valid: {} ({:.2f}%)".format(DPV_valid_count, round(DPV_valid_percentage, 2)))

    full_valid_count = (
        (result_df['DPV'].apply(lambda x: str(x)).str.lower() == "true") &
        (result_df['Resolved'].apply(lambda x: str(x)).str.lower() == "true") &
        (result_df['AddressType'] == "STANDARDIZED") &
        (result_df['Interpolated'].apply(lambda x: str(x)).str.lower() != "true") &
        (result_df['customerMessages']).apply(lambda x: len(str(x)) < 5)
    ).sum()
    full_valid_percentage = (full_valid_count / total_addresses) * 100
    output_lines.append("Nb full valid: {} ({:.2f}%)".format(full_valid_count, round(full_valid_percentage, 2)))

    postcode_changes_count = (result_df['postcode'].astype(int) != result_df['postcode_R'].astype(int)).sum()
    postcode_changes_percentage = (postcode_changes_count / total_addresses) * 100
    output_lines.append("Nb postcode changes: {} ({:.2f}%)".format(postcode_changes_count, round(postcode_changes_percentage, 2)))

    city_changes_count = (result_df['city_clean'].apply(lambda x: str(x)).str.upper() != result_df['city_R'].str.upper()).sum()
    city_changes_percentage = (city_changes_count / total_addresses) * 100
    output_lines.append("Nb city changes: {} ({:.2f}%)".format(city_changes_count, round(city_changes_percentage, 2)))

    missing_apt_count = (result_df['SuiteRequiredButMissing'].apply(lambda x: str(x)).str.lower() == "true").sum()
    missing_apt_percentage = (missing_apt_count / total_addresses) * 100
    output_lines.append("Nb Missing Apt: {} ({:.2f}%)".format(missing_apt_count, round(missing_apt_percentage, 2)))

    invalid_apt_count = (result_df['InvalidSuiteNumber'].apply(lambda x: str(x)).str.lower() == "true").sum()
    invalid_apt_percentage = (invalid_apt_count / total_addresses) * 100
    output_lines.append("Nb Invalid Apt: {} ({:.2f}%)".format(invalid_apt_count, round(invalid_apt_percentage, 2)))

    non_empty_messages_count = result_df['customerMessages'].apply(lambda x: len(x) > 5).sum()
    non_empty_messages_percentage = (non_empty_messages_count / total_addresses) * 100
    output_lines.append("Nb Error messages: {} ({:.2f}%)".format(non_empty_messages_count, round(non_empty_messages_percentage, 2)))

    error_messages = result_df['customerMessages'].unique()

    for error in error_messages:
        error_count = (result_df['customerMessages'] == error).sum()
        error_percentage = (error_count / total_addresses) * 100
        output_lines.append("Nb Error messages {}: {} ({:.2f}%)".format(error, error_count, round(error_percentage, 2)))

    # Print the information
    for line in output_lines:
        print(line)

    if save:
        # Save the information to a file
        with open(path[:-25] + "post-FedExAPI-successful-stats.txt", 'w') as file:
            file.write('\n'.join(output_lines))

    return

def addressissues_count(df,RootCauses):
    total=len(df)
    print("\n Perc of Root Cause: ",round(len(df[df['FINAL_ROOT_CAUSE'].isin(RootCauses)])/total*100,2))
    for root in RootCauses:
        print(f"Count of {root}: ",round(len(df[df['FINAL_ROOT_CAUSE']==root])/total*100,2))
    return

def show_OTP_split(df):
    counts = df.groupby('OTP_GROUP').size()
    percentages = round((counts / len(df)) * 100,2)

    OTP_df = pd.DataFrame({'Counts': counts, 'Percentages': percentages})
    print(OTP_df)
    return

def add_street_comparison(df):
    # Combine street_1_clean and street_2_clean into a tuple for street_list
    df['street_list'] = df.apply(
        lambda row: tuple(word_tokenize(process_street(f"{row['street_1_clean']} {row['street_2_clean']}" if pd.notna(row['street_2_clean']) else row['street_1_clean'])))
        if pd.notna(row['street_1_clean']) else (),
        axis=1
    )

    # Combine street_R and street_2_R into a tuple for street_list_R
    df['street_list_R'] = df.apply(
        lambda row: tuple(word_tokenize(process_street(f"{row['street_R']} {row['street_2_R']}" if pd.notna(row['street_2_R']) else row['street_R'])))
        if pd.notna(row['street_R']) else (),
        axis=1
    )

    df['street_diff']=(df['street_list']!=df['street_list_R'])
    df['street_cleaning'] = df.apply(lambda row: set(row['street_list_R']).issubset(set(row['street_list'])), axis=1)
    df['street_adding'] = df.apply(lambda row: set(row['street_list']).issubset(set(row['street_list_R'])), axis=1)
    
    return df

# Function to remove punctuation and apply equivalence mapping
def process_street(text):
    equivalence_dict = {
    'unit': '',
    'apt':'',
    'ste':'',
    'spc':'',
    'square':'sq',
    'street': 'st',
    'lane':'ln',
    'circle':'cir',
    'drive':'dr',
    'parkway':'pkwy',
    'place':'pl',
    'avenue':'ave',
    'space':'spc',
    'boulevard':'blvd',
    'terrace':'ter',
    'trailer':'trlr',
    'south':'s',
    'north':'n',
    'west':'w',
    'east':'e',
    'court':'ct',
    'road':'rd',
    'route':'rte',
    'plaza':'plz',
    'highway':'hwy',
    'trail':'trl',
    'ridge':'rdg',
    'second':'2nd',
    'first':'1st',
    'third':'3rd',
    'fourth':'4th',
    'fifth':'5th',
    'sixth':'6th'
    }
    # Remove punctuation and convert to lowercase
    text_without_punctuation = text.translate(str.maketrans('', '', string.punctuation)).lower()
    
    # Apply equivalence mapping
    for key, value in equivalence_dict.items():
        text_without_punctuation = text_without_punctuation.replace(key, value)
    
    return text_without_punctuation