import os
from ftplib import parse150
import pandas as pd
import numpy as np
import time
import json
from simple_term_menu import TerminalMenu, main
#from dask import dataframe as df1
  
# time taken to read data

generate_new_percentile_tables = True

if generate_new_percentile_tables:
    s_time = time.time()
    pd.set_option('display.max_columns', None)
    #df = pd.read_csv("openpowerlifting-2022-02-22-8fc7c9ba.csv")
    df = pd.read_csv("openipf-2022-02-22-8fc7c9ba.csv")
    e_time = time.time()
    print("Read without chunks: ", (e_time-s_time), "seconds")
  


def generate_tables():
    pass

# data
#print(df.sample(50))
#print(df.columns.tolist())
#print(len(df))

#me = df[df['Name'] == 'Daniel Menz']
#print(me)


#filter_weight_class = '74' 
#filter_lift = 'TotalKg' #Best3SquatKg, Best3BenchKg, Best3DeadliftKg TotalKg
#target_lift = 501 # your inputted lift





def calc_percentile(weight_class_full_data, filter_lift_input, target_lift, ):
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

        filter_event = filter_event_dict[filter_lift]

        res = []

        filtered_df = df[(df['Equipment'] == 'Raw')  
                    & (df['BodyweightKg'] > weight_class_full_data['WeightMin']) 
                    & (df['BodyweightKg'] <= weight_class_full_data['WeightMax'])  
                    & (df['Equipment'] == 'Raw')  
                    & (df['Sex'] == weight_class_full_data['Sex'])
                    & (df['Event'].isin(filter_event))
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

        # percentiles = [100, 90, 80, 70, 60, 50 ,40 ,30 ,20, 10]

        percentiles = [i for i in range(100,0, -1)]

        for percentile in percentiles:
            location = int(entry_size * (1 - (percentile / 100)))
            entry = (sorted_df.iloc[location,:])
            print(f"{percentile} - {entry[filter_lift]} - {entry['Name']} - {entry_size - int(percentile/100*entry_size)}")
            res.append((percentile, entry[filter_lift]))

        lift_label_lookup = {
            'Best3SquatKg': 'Squat',
            'Best3BenchKg': 'Bench',
            'Best3DeadliftKg': 'Deadlift',
            'TotalKg': 'Total'
        }

        class_percentiles[lift_label_lookup[filter_lift]] = res
        print()   

    # Output to file
    output_file_name = weight_class_full_data['WeightClass'] +'_percentiles.json'
    with open(output_file_name, "w") as outfile:
        outfile.write(json.dumps(class_percentiles))

    #print(class_percentiles)

    for pair in class_percentiles[lift_label_lookup[filter_lift_input]]:
        print(pair)
        if pair[1] > target_lift:
            pass
        else:
            print(f"Your lift of {target_lift}kg")
            print(f"{pair[0]}% percentile for {filter_lift_input} for {weight_class_full_data['WeightClass']}kg class")
            break


def main():


    weight_class_dict = {
    '53' : {"WeightClass": "53", "Sex" : 'M', "WeightMin": 0.0, "WeightMax": 53.0},
    '59' : {"WeightClass": "59", "Sex" : 'M', "WeightMin": 52.0, "WeightMax": 59.0},
    '66' : {"WeightClass": "66", "Sex" : 'M', "WeightMin": 59.0, "WeightMax": 66.0},
    '74' : {"WeightClass": "74", "Sex" : 'M', "WeightMin": 66.0, "WeightMax": 74.0},
    '83' : {"WeightClass": "83", "Sex" : 'M', "WeightMin": 74.0, "WeightMax": 83.0},
    '93' : {"WeightClass": "93", "Sex" : 'M', "WeightMin": 83.0, "WeightMax": 93.0},
    '105' : {"WeightClass": "105", "Sex" : 'M', "WeightMin": 93.0, "WeightMax": 105.0},
    '120' : {"WeightClass": "120", "Sex" : 'M', "WeightMin": 105.0, "WeightMax": 120.0},
    '120+' : {"WeightClass": "120+", "Sex" : 'M', "WeightMin": 120.0, "WeightMax": 999.0},
    '43' : {"WeightClass": "43", "Sex" : 'F', "WeightMin": 0.0, "WeightMax": 43.0},
    '47' : {"WeightClass": "47", "Sex" : 'F', "WeightMin": 43.0, "WeightMax": 47.0},
    '52' : {"WeightClass": "52", "Sex" : 'F', "WeightMin": 47.0, "WeightMax": 52.0},
    '57' : {"WeightClass": "57", "Sex" : 'F', "WeightMin": 52.0, "WeightMax": 57.0},
    '63' : {"WeightClass": "63", "Sex" : 'F', "WeightMin": 57.0, "WeightMax": 63.0},
    '72' : {"WeightClass": "72", "Sex" : 'F', "WeightMin": 63.0, "WeightMax": 72.0},
    '84' : {"WeightClass": "84", "Sex" : 'F', "WeightMin": 72.0, "WeightMax": 84.0},
    '84+' : {"WeightClass": "84+", "Sex" : 'F', "WeightMin": 84.0, "WeightMax": 999.0},
}

# 17 weight classes, make a per
   
    main_menu = TerminalMenu(['Start', "Exit"], title = "-------------------")
    main_menu_index = None

    while main_menu_index != 1:
        #os.system('clear')
        main_menu_index = main_menu.show()
        
        if main_menu_index == 0:
            weight_classes = ['59','66','74','83','93','105','120','120+','----','43','47','52','57','63','72','84','84+']
            menu_weight_class = TerminalMenu(weight_classes)
            weight_class_selection = menu_weight_class.show()

            lift_options = ['Best3SquatKg', 'Best3BenchKg', 'Best3DeadliftKg', 'TotalKg']
            lift_options_labels = ['Squat', 'Bench', 'Deadlift', 'Total']
            menu_lift = TerminalMenu(lift_options_labels)
            lift_selection_index = menu_lift.show()
            
            target_weight = float(input("Your lift (kg)?: "))

            weight_class = weight_classes[weight_class_selection]
            weight_class_full = weight_class_dict[weight_class]
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










# // IPF Men.
# assert_kg_to_lbs("53", "116");
# assert_kg_to_lbs("59", "130");
# assert_kg_to_lbs("66", "145");
# assert_kg_to_lbs("74", "163");
# assert_kg_to_lbs("83", "183");
# assert_kg_to_lbs("93", "205");
# assert_kg_to_lbs("105", "231");
# assert_kg_to_lbs("120", "264");
# assert_kg_to_lbs("120+", "264+")

# // IPF Women.
# assert_kg_to_lbs("43", "94");
# assert_kg_to_lbs("47", "103");
# assert_kg_to_lbs("52", "114");
# assert_kg_to_lbs("57", "125");
# assert_kg_to_lbs("63", "138");
# assert_kg_to_lbs("72", "158");
# assert_kg_to_lbs("84", "185");
# assert_kg_to_lbs("84+", "185+");
