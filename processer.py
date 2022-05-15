import os
from ftplib import parse150
import pandas as pd
import numpy as np
import time
import json
import sys
from simple_term_menu import TerminalMenu, main
#from dask import dataframe as df1
  
generate_new_percentile_tables = False
generate_full_details = False

output_file = 'all_data.json'

weight_class_dict = {
    '53_M' : {"WeightClass": "53", "Sex" : 'M', "Equip": "Raw", "Tested": "tested", "WeightMin": 0.0, "WeightMax": 53.0},
    '59_M' : {"WeightClass": "59", "Sex" : 'M', "Equip": "Raw", "Tested": "tested",  "WeightMin": 53.0, "WeightMax": 59.0},
    '66_M' : {"WeightClass": "66", "Sex" : 'M', "Equip": "Raw", "Tested": "tested",  "WeightMin": 59.0, "WeightMax": 66.0},
    '74_M' : {"WeightClass": "74", "Sex" : 'M', "Equip": "Raw", "Tested": "tested",  "WeightMin": 66.0, "WeightMax": 74.0},
    '83_M' : {"WeightClass": "83", "Sex" : 'M', "Equip": "Raw", "Tested": "tested",  "WeightMin": 74.0, "WeightMax": 83.0},
    '93_M' : {"WeightClass": "93", "Sex" : 'M', "Equip": "Raw", "Tested": "tested",  "WeightMin": 83.0, "WeightMax": 93.0},
    '105_M' : {"WeightClass": "105", "Sex" : 'M', "Equip": "Raw", "Tested": "tested",  "WeightMin": 93.0, "WeightMax": 105.0},
    '120_M' : {"WeightClass": "120", "Sex" : 'M', "Equip": "Raw", "Tested": "tested",  "WeightMin": 105.0, "WeightMax": 120.0},
    '120+_M' : {"WeightClass": "120+", "Sex" : 'M', "Equip": "Raw", "Tested": "tested",  "WeightMin": 120.0, "WeightMax": 999.0},
    '43_F' : {"WeightClass": "43", "Sex" : 'F', "Equip": "Raw", "Tested": "tested",  "WeightMin": 0.0, "WeightMax": 43.0},
    '47_F' : {"WeightClass": "47", "Sex" : 'F', "Equip": "Raw", "Tested": "tested",  "WeightMin": 43.0, "WeightMax": 47.0},
    '52_F' : {"WeightClass": "52", "Sex" : 'F', "Equip": "Raw", "Tested": "tested",  "WeightMin": 47.0, "WeightMax": 52.0},
    '57_F' : {"WeightClass": "57", "Sex" : 'F', "Equip": "Raw", "Tested": "tested",  "WeightMin": 52.0, "WeightMax": 57.0},
    '63_F' : {"WeightClass": "63", "Sex" : 'F', "Equip": "Raw", "Tested": "tested",  "WeightMin": 57.0, "WeightMax": 63.0},
    '69_F' : {"WeightClass": "69", "Sex" : 'F', "Equip": "Raw", "Tested": "tested",  "WeightMin": 63.0, "WeightMax": 69.0},
    '76_F' : {"WeightClass": "76", "Sex" : 'F', "Equip": "Raw", "Tested": "tested",  "WeightMin": 69.0, "WeightMax": 76.0},
    '84_F' : {"WeightClass": "84", "Sex" : 'F', "Equip": "Raw", "Tested": "tested",  "WeightMin": 76.0, "WeightMax": 84.0},
    '84+_F' : {"WeightClass": "84+", "Sex" : 'F', "Equip": "Raw", "Tested": "tested",  "WeightMin": 84.0, "WeightMax": 999.0},
}

weight_class_dict_untested = {
    '52_M' : {"WeightClass": "52", "Sex" : 'M', "Equip": "Raw", "Tested": "untested", "WeightMin": 0.0, "WeightMax": 52.0},
    '56_M' : {"WeightClass": "56", "Sex" : 'M', "Equip": "Raw", "Tested": "untested",  "WeightMin": 52.0, "WeightMax": 56.0},
    '60_M' : {"WeightClass": "60", "Sex" : 'M', "Equip": "Raw", "Tested": "untested",  "WeightMin": 56.0, "WeightMax": 60.0},
    '67.5_M' : {"WeightClass": "67.5", "Sex" : 'M', "Equip": "Raw", "Tested": "untested",  "WeightMin": 60.0, "WeightMax": 67.5},
    '75_M' : {"WeightClass": "75", "Sex" : 'M', "Equip": "Raw", "Tested": "untested",  "WeightMin": 67.5, "WeightMax": 75.0},
    '82.5_M' : {"WeightClass": "82.5", "Sex" : 'M', "Equip": "Raw", "Tested": "untested",  "WeightMin": 75.0, "WeightMax": 82.5},
    '90_M' : {"WeightClass": "90", "Sex" : 'M', "Equip": "Raw", "Tested": "untested",  "WeightMin": 82.5, "WeightMax": 90.0},
    '100_M' : {"WeightClass": "100", "Sex" : 'M', "Equip": "Raw", "Tested": "untested",  "WeightMin": 90.0, "WeightMax": 100.0},
    '110_M' : {"WeightClass": "110", "Sex" : 'M', "Equip": "Raw", "Tested": "untested",  "WeightMin": 100.0, "WeightMax": 110.0},
    '125_M' : {"WeightClass": "125", "Sex" : 'M', "Equip": "Raw", "Tested": "untested",  "WeightMin": 110.0, "WeightMax": 125.0},
    '140_M' : {"WeightClass": "140", "Sex" : 'M', "Equip": "Raw", "Tested": "untested",  "WeightMin": 125.0, "WeightMax": 140.0},
    '140+_M' : {"WeightClass": "140+", "Sex" : 'M', "Equip": "Raw", "Tested": "untested",  "WeightMin": 140.0, "WeightMax": 999.0},
    '44_F' : {"WeightClass": "44", "Sex" : 'F', "Equip": "Raw", "Tested": "untested",  "WeightMin": 0.0, "WeightMax": 44.0},
    '48_F' : {"WeightClass": "48", "Sex" : 'F', "Equip": "Raw", "Tested": "untested",  "WeightMin": 44.0, "WeightMax": 48.0},
    '52_F' : {"WeightClass": "52", "Sex" : 'F', "Equip": "Raw", "Tested": "untested",  "WeightMin": 48.0, "WeightMax": 52.0},
    '56_F' : {"WeightClass": "56", "Sex" : 'F', "Equip": "Raw", "Tested": "untested",  "WeightMin": 52.0, "WeightMax": 56.0},
    '60_F' : {"WeightClass": "60", "Sex" : 'F', "Equip": "Raw", "Tested": "untested",  "WeightMin": 56.0, "WeightMax": 60.0},
    '67.5_F' : {"WeightClass": "67.5", "Sex" : 'F', "Equip": "Raw", "Tested": "untested",  "WeightMin": 60.0, "WeightMax": 67.5},
    '75_F' : {"WeightClass": "75", "Sex" : 'F', "Equip": "Raw", "Tested": "untested",  "WeightMin": 67.5, "WeightMax": 75.0},
    '82.5_F' : {"WeightClass": "82.5", "Sex" : 'F', "Equip": "Raw", "Tested": "untested",  "WeightMin": 75.0, "WeightMax": 82.5},
    '90_F' : {"WeightClass": "90", "Sex" : 'F', "Equip": "Raw", "Tested": "untested",  "WeightMin": 82.5, "WeightMax": 90.0},
    '90+_F' : {"WeightClass": "90+", "Sex" : 'F', "Equip": "Raw", "Tested": "untested",  "WeightMin": 90.0, "WeightMax": 999.0},
}

weight_class_dict_untested_wraps = {
    'W52_M' : {"WeightClass": "52", "Sex" : 'M', "Equip": "Wraps", "Tested": "untested", "WeightMin": 0.0, "WeightMax": 52.0},
    'W56_M' : {"WeightClass": "56", "Sex" : 'M', "Equip": "Wraps", "Tested": "untested",  "WeightMin": 52.0, "WeightMax": 56.0},
    'W60_M' : {"WeightClass": "60", "Sex" : 'M', "Equip": "Wraps", "Tested": "untested",  "WeightMin": 56.0, "WeightMax": 60.0},
    'W67.5_M' : {"WeightClass": "67.5", "Sex" : 'M', "Equip": "Wraps", "Tested": "untested",  "WeightMin": 60.0, "WeightMax": 67.5},
    'W75_M' : {"WeightClass": "75", "Sex" : 'M', "Equip": "Wraps", "Tested": "untested",  "WeightMin": 67.5, "WeightMax": 75.0},
    'W82.5_M' : {"WeightClass": "82.5", "Sex" : 'M', "Equip": "Wraps", "Tested": "untested",  "WeightMin": 75.0, "WeightMax": 82.5},
    'W90_M' : {"WeightClass": "90", "Sex" : 'M', "Equip": "Wraps", "Tested": "untested",  "WeightMin": 82.5, "WeightMax": 90.0},
    'W100_M' : {"WeightClass": "100", "Sex" : 'M', "Equip": "Wraps", "Tested": "untested",  "WeightMin": 90.0, "WeightMax": 100.0},
    'W110_M' : {"WeightClass": "110", "Sex" : 'M', "Equip": "Wraps", "Tested": "untested",  "WeightMin": 100.0, "WeightMax": 110.0},
    'W125_M' : {"WeightClass": "125", "Sex" : 'M', "Equip": "Wraps", "Tested": "untested",  "WeightMin": 110.0, "WeightMax": 125.0},
    'W140_M' : {"WeightClass": "140", "Sex" : 'M', "Equip": "Wraps", "Tested": "untested",  "WeightMin": 125.0, "WeightMax": 140.0},
    'W140+_M' : {"WeightClass": "140+", "Sex" : 'M', "Equip": "Wraps", "Tested": "untested",  "WeightMin": 140.0, "WeightMax": 999.0},
    'W44_F' : {"WeightClass": "44", "Sex" : 'F', "Equip": "Wraps", "Tested": "untested",  "WeightMin": 0.0, "WeightMax": 44.0},
    'W48_F' : {"WeightClass": "48", "Sex" : 'F', "Equip": "Wraps", "Tested": "untested",  "WeightMin": 44.0, "WeightMax": 48.0},
    'W52_F' : {"WeightClass": "52", "Sex" : 'F', "Equip": "Wraps", "Tested": "untested",  "WeightMin": 48.0, "WeightMax": 52.0},
    'W56_F' : {"WeightClass": "56", "Sex" : 'F', "Equip": "Wraps", "Tested": "untested",  "WeightMin": 52.0, "WeightMax": 56.0},
    'W60_F' : {"WeightClass": "60", "Sex" : 'F', "Equip": "Wraps", "Tested": "untested",  "WeightMin": 56.0, "WeightMax": 60.0},
    'W67.5_F' : {"WeightClass": "67.5", "Sex" : 'F', "Equip": "Wraps", "Tested": "untested",  "WeightMin": 60.0, "WeightMax": 67.5},
    'W75_F' : {"WeightClass": "75", "Sex" : 'F', "Equip": "Wraps", "Tested": "untested",  "WeightMin": 67.5, "WeightMax": 75.0},
    'W82.5_F' : {"WeightClass": "82.5", "Sex" : 'F', "Equip": "Wraps", "Tested": "untested",  "WeightMin": 75.0, "WeightMax": 82.5},
    'W90_F' : {"WeightClass": "90", "Sex" : 'F', "Equip": "Wraps", "Tested": "untested",  "WeightMin": 82.5, "WeightMax": 90.0},
    'W90+_F' : {"WeightClass": "90+", "Sex" : 'F', "Equip": "Wraps", "Tested": "untested",  "WeightMin": 90.0, "WeightMax": 999.0},
}

youth_divisions = [
        'MR-T1',
        'MR-T2',
        'MR-T3',
        'MR-Y1',
        'MR-Y2',
        'MR-Y3',
        'FR-T1',
        'FR-T2',
        'FR-T3',
        'FR-Y1',
        'FR-Y2',
        'FR-Y3',
        'Sub-Juniors',
        'Teen',
        ]

lift_label_lookup = {
                'Best3SquatKg': 'Squat',
                'Best3BenchKg': 'Bench',
                'Best3DeadliftKg': 'Deadlift',
                'TotalKg': 'Total'
            }

def generate_tables():

    # Download information and unzip
    tested_data_csv = "openipf-latest.csv"
    untested_data_csv = "openpowerlifting-latest.csv"

    # Create empty results file if it doesn't exist
    if not os.path.exists('all_data1.json'):
        with open('output_file', "w+") as outfile:
            outfile.write(json.dumps({}))

    generate_tables_per_file(untested_data_csv, weight_class_dict_untested_wraps)
    generate_tables_per_file(untested_data_csv, weight_class_dict_untested)
    generate_tables_per_file(tested_data_csv, weight_class_dict)

def generate_tables_per_file(csv_file, weight_classes):

    s_time = time.time()
    pd.set_option('display.max_columns', None)
    #df = pd.read_csv("openpowerlifting-2022-02-22-8fc7c9ba.csv")
    df = pd.read_csv(csv_file)
    e_time = time.time()
    print("Read without chunks: ", (e_time-s_time), "seconds")
    
    for weight_class_full_data in weight_classes.values():
                
        class_percentiles = {
            "Squat" : {},
            "Bench" : {},
            "Deadlift": {},
            "Total": {}
        }

        for filter_lift in ['Best3SquatKg', 'Best3BenchKg', 'Best3DeadliftKg', 'TotalKg']:
            filter_event_dict = {
                'Best3SquatKg': ['SBD'],
                'Best3BenchKg': ['B','BD','SBD'],
                'Best3DeadliftKg': ['D','BD','SBD'],
                'TotalKg': ['SBD']
            }

            filter_equipment_dict = {
                'Best3SquatKg':  [weight_class_full_data['Equip']],
                'Best3BenchKg': ['Raw', 'Wraps'],
                'Best3DeadliftKg': ['Raw', 'Wraps'],
                'TotalKg':  [weight_class_full_data['Equip']]
            }

            # ----Current
            # Untested Sleeves
            # SQ - Sleeves, SDB
            # BP - Sleeves, SDB BD B
            # DL - Sleeves, SBD BD D
            # TL - Sleeves, SBD

            # Untested Wraps
            # SQ - Wraps, SDB
            # BP - Wraps, SDB 
            # DL - Wraps, SBD 
            # TL - Wraps, SBD

            # ------ Ideal
            # Untested Sleeves
            # SQ - Sleeves, SDB
            # BP - Raw + Wraps, SDB BD B
            # DL - Raw + Wraps, SBD BD D
            # TL - Sleeves, SBD

            # Untested Wraps
            # SQ - Wraps, SDB
            # BP - Raw + Wraps, SDB BD B
            # DL - Raw + Wraps, SBD BD D
            # TL - Wraps, SBD

            filter_event = filter_event_dict[filter_lift]
            filter_equipment = filter_equipment_dict[filter_lift]
            res = []

            filtered_df = df[(df['Event'].isin(filter_event))
                        & (df['Equipment'].isin(filter_equipment))  
                        & (df['BodyweightKg'] > weight_class_full_data['WeightMin']) 
                        & (df['BodyweightKg'] <= weight_class_full_data['WeightMax'])  
                        & (df['Sex'] == weight_class_full_data['Sex']) 
                        & (df['Place'] != 'DQ') 
                        & (~df['Division'].isin(youth_divisions))  
                        & (np.isnan(df[filter_lift]) == False)]

            #print(len(filtered_df))
            selection = filtered_df.loc[:,['Name','BodyweightKg', 'Event', filter_lift, 'Age']]

            # Need to get the best total for each Name
            sorted_df = selection.sort_values(filter_lift, ascending=False).drop_duplicates(['Name'])
            entry_size = len(sorted_df)
            print(f"{entry_size} entries in selected class") # Number of entries in that category, not name disambiguated
            print(filter_lift)
            print(weight_class_full_data['WeightClass'])


            percentiles = [i for i in range(100,0, -1)]

            for percentile in percentiles:
                location = int(entry_size * (1 - (percentile / 100)))
                entry = (sorted_df.iloc[location,:])
                #print(f"{percentile} - {entry[filter_lift]} - {entry['Name']} - {entry_size - int(percentile/100*entry_size)}")
                
                # full detail or shortened details
                if generate_full_details:
                    res.append([percentile, entry[filter_lift], entry_size - int(percentile/100*entry_size), entry['Name']])
                else:
                    res.append([percentile, entry[filter_lift], entry_size - int(percentile/100*entry_size)])


            class_percentiles[lift_label_lookup[filter_lift]] = res
            print()


        # output to one file (read in current and then add to dict and append to file)
        with open('output_file', 'r') as file:
            cur_percentile_data = json.load(file)

        cur_percentile_data[weight_class_full_data['Sex'] + weight_class_full_data['WeightClass'] +weight_class_full_data['Equip']] = class_percentiles   

        with open('output_file', "w") as outfile:
            outfile.write(json.dumps(cur_percentile_data))
    
    pass



def calc_percentile(weight_class_full_data, filter_lift_input, target_lift, ):
   
    # load in correct file
    input_file_name = weight_class_full_data['Sex'] + weight_class_full_data['WeightClass'] + weight_class_full_data['Equip'] \
                         + '_percentiles.json'

    class_dict_key = weight_class_full_data['Sex'] + weight_class_full_data['WeightClass'] + weight_class_full_data['Equip']
    
    with open('output_file') as file:
            all_percentile_data = json.load(file)

    for data in (all_percentile_data[class_dict_key][lift_label_lookup[filter_lift_input]]):
        print(data)

    print('------------')

    for pair in all_percentile_data[class_dict_key][lift_label_lookup[filter_lift_input]]:
        print(pair)
        if pair[1] > target_lift:
            pass
        else:
            print(f"Your lift of {target_lift}kg")
            print(f"{pair[0]}% percentile for {filter_lift_input} for {weight_class_full_data['WeightClass']}kg class")
            break

def show_class():
    weight_classes_all = list(weight_class_dict.keys()) + list(weight_class_dict_untested.keys())
    gender_menu = TerminalMenu(['Male', "Female"])
    gender_selection_index = gender_menu.show()
    gender_selection = ['M','F'][gender_selection_index]

    weight_classes = [e for e in weight_classes_all if gender_selection in e]
    menu_weight_class = TerminalMenu(weight_classes, title = "Choose Weight Class")
    weight_class_selection = menu_weight_class.show()
    weight_class = weight_classes[weight_class_selection]

    try:
        weight_class_full_data = weight_class_dict_untested[weight_class]
    except:
        weight_class_full_data = weight_class_dict[weight_class]

    
    with open('output_file') as file:
        all_percentile_data = json.load(file)

    class_dict_key = weight_class_full_data['Sex'] + weight_class_full_data['WeightClass'] + weight_class_full_data['Equip']

    for i in range(0,100):
        temp = ""
        for k,v in (all_percentile_data[class_dict_key]).items():
            temp +=f"{k} {v[i][1]:6} | " 
        print(f"{100-i:3}%  {temp}")

    exit()    

def main():
    os.system('clear')
    generate_new_percentile_tables = False

    if len(sys.argv) == 2 and sys.argv[1] == 'gen':
        generate_new_percentile_tables = True

    if len(sys.argv) == 2 and sys.argv[1] == 'class':
        show_class()  

    if generate_new_percentile_tables:
        s_time = time.time()
        generate_tables()    
        e_time = time.time()
        print("Generating the datatables took: ", (e_time-s_time), "seconds")
        exit()

    main_menu = TerminalMenu(['Start', "Exit"], title = "-------------------")
    main_menu_index = None

    first_run = True

    while main_menu_index != 1:
        #os.system('clear')
        if first_run:
            first_run = False
            main_menu_index = 0
        else:
            main_menu_index = main_menu.show()
        
        if main_menu_index == 0:
            os.system('clear')
            
            gender_menu = TerminalMenu(['Male', "Female"])
            gender_selection_index = gender_menu.show()
            gender_selection = ['M','F'][gender_selection_index]

            tested_menu = TerminalMenu(['Tested', "Untested"])
            tested_selection_index = tested_menu.show()
            tested_selection = ['tested','untested'][tested_selection_index]

            if tested_selection == 'tested':
                weight_classes_selected =  list(weight_class_dict.keys())
            else:
                weight_classes_selected =  list(weight_class_dict_untested.keys())
                
            weight_classes = [e for e in weight_classes_selected if gender_selection in e]

            menu_weight_class = TerminalMenu(weight_classes, title = "Choose Weight Class")
            weight_class_selection = menu_weight_class.show()

            lift_options = ['Best3SquatKg', 'Best3BenchKg', 'Best3DeadliftKg', 'TotalKg']
            lift_options_labels = ['Squat', 'Bench', 'Deadlift', 'Total']
            menu_lift = TerminalMenu(lift_options_labels)
            lift_selection_index = menu_lift.show()
            
            target_weight = float(input("Your lift (kg)?: "))

            weight_class = weight_classes[weight_class_selection]
            
            if tested_selection == 'tested':
                weight_class_full = weight_class_dict[weight_class]
            else:
                weight_class_full = weight_class_dict_untested[weight_class]

            print(weight_class)
            print(weight_class_full)

            lift_selection = lift_options[lift_selection_index]
            print(lift_selection)
            print(target_weight)
            calc_percentile(weight_class_full, lift_selection, target_weight)
            
        
    exit()


if __name__== '__main__':
    main()

#breakpoint()
