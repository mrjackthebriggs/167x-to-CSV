import csv
import avro.datafile
import avro.io

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
                circuits.extend(v)
            else:    
                for val in v:
                    list_flatten, list_circs = flatten_dict(val, new_key, sep=sep)
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
                flat_record,curcs = flatten_dict(record)
                
                for circ in curcs[0][0]:
                    flat_circ_rec,_ = flatten_dict(circ, parent_key='')
                    circ_record = flat_circ_rec | flat_record 
                    print("\n\n\naaaaa",circ_record)
                    all_flattened_rows.append(circ_record)
        

    if not all_flattened_rows:
        print("No data found.")
        return

    # Identify all unique keys for CSV headers
    keys = set().union(*(d.keys() for d in all_flattened_rows))
    # Filter out list objects that weren't expanded to keep CSV clean
    fieldnames = [k for k in keys if not isinstance(all_flattened_rows[0].get(k), list)]

    # Write to CSV
    with open(csv_path, 'w', newline='', encoding='utf-8') as output_file:
        dict_writer = csv.DictWriter(output_file, fieldnames=fieldnames, extrasaction='ignore')
        dict_writer.writeheader()
        dict_writer.writerows(all_flattened_rows)

    print(f"Successfully created {csv_path} with {len(fieldnames)} columns.")

if __name__ == "__main__":
    process_avro_to_csv('Test_Tezt.F167x', 'output_report.csv')
