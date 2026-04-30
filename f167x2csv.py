# written in Python 3.14.4 by Jack Briggs on the 29/04/26

import csv
import avro.datafile
import avro.io
import os
import re

def flatten_dict(d, parent_key='', sep='_'):
    """
    Recursively flattens a dictionary. 
    Nested keys are joined by the separator (e.g., client_address_city).
    """
    items = []
    circuits = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            dict_flatten, dict_circs = flatten_dict(v, new_key, sep=sep)
            items.extend(dict_flatten.items())

            if len(dict_circs)>0:
                circuits.append(dict_circs)

        elif isinstance(v, list):
            if k == 'circuits':
                for val in v:
                    if len(val.get('testPoints')) > 0:
                        for tp in val.get('testPoints'):
                            tp["tp_name"] = tp.pop("name")
                            circuits.append(tp)
                    else:
                        circuits.append(val)
            else:    
                for i,val in enumerate(v):

                    get_name = val.get('name');
                    get_funct = val.get('function')
                    id_str = [str(i)]
                    #if get_name: id_str.append(get_name)
                    #if get_funct: id_str.append(get_funct)
                    id = '-'.join(id_str)

                    list_flatten, list_circs = flatten_dict(val, id + '-' + new_key, sep=sep)
                    items.extend(list_flatten.items())

                    if len(list_circs) > 0:
                        circuits.append(list_circs)
        else:
            items.append((new_key, v))
    return (dict(items),circuits)

def process_avro_to_csv(avro_path, csv_path):
    all_flattened_rows = []

    with open(avro_path, 'rb') as f:
        with avro.datafile.DataFileReader(f, avro.io.DatumReader()) as reader:
            for record in reader:

                record = record['data']

                flat_record,curcs = flatten_dict(record, parent_key="")
                
                for circ in curcs[0][0]:
                    flat_circ_rec,_ = flatten_dict(circ, parent_key='')
                    circ_record = flat_circ_rec | flat_record 
                    # print("\n\n\naaaaa",circ_record)
                    all_flattened_rows.append(circ_record)
        

    if not all_flattened_rows:
        print("No data found.")
        return

    # Identify all unique keys for CSV headers
    keys = set().union(*(d.keys() for d in all_flattened_rows))

    # remove all keys that never have a value
    remove_keys = []
    for i in keys:
        key_list = []
        for j in all_flattened_rows:
            jget = str(j.get(i))
            key_list.append(jget == 'None' or jget == '')

        if all(key_list):
            remove_keys.append(i)

    keys = list({*keys}.difference({*remove_keys}))

    print(f"removed {str(len(remove_keys))} blank key/s")

    # Filter out list objects that weren't expanded to keep CSV clean
    fieldnames = sorted([k for k in keys], reverse=True)

    # Write to CSV
    with open(csv_path, 'w', newline='', encoding='utf-8') as output_file:
        dict_writer = csv.DictWriter(output_file, fieldnames=fieldnames, extrasaction='ignore')
        dict_writer.writeheader()
        dict_writer.writerows(all_flattened_rows)

    print(f"Successfully created {csv_path} with {len(fieldnames)} columns.")

if __name__ == "__main__":
    dir_path = os.path.dirname(os.path.realpath(__file__))
    dir_stuff = os.listdir()
    f167x_files = [j.string for j in [re.search('.F167(x|X)',i) for i in dir_stuff] if j != None]

    file_names = [i.split('.')[0] for i in f167x_files]
   
    for f in file_names:
        process_avro_to_csv(f + '.F167x', f + '.csv')
