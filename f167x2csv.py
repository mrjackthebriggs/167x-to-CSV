import csv
import avro.datafile
import avro.io

def flatten_dict(d, parent_key='', sep='_'):
    """
    Recursively flattens a dictionary. 
    Nested keys are joined by the separator (e.g., client_address_city).
    """
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            for val in v:
                items.extend(flatten_dict(val, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)

def process_avro_to_csv(avro_path, csv_path):
    all_flattened_rows = []

    with open(avro_path, 'rb') as f:
        with avro.datafile.DataFileReader(f, avro.io.DatumReader()) as reader:
            for record in reader:

                record = record['data']
                flat_record = flatten_dict(record)
                
                if 'distributionBoards' in record:
                    for db in record['distributionBoards']:
                        db_flat = flatten_dict(db, parent_key='db')
                        combined = {**flat_record, **db_flat}
                        combined.pop('distributionBoards', None)
                        all_flattened_rows.append(combined)
                else:
                    all_flattened_rows.append(flat_record)
        

    if not all_flattened_rows:
        print("No data found.")
        return

    # Identify all unique keys for CSV headers
    keys = set().union(*(d.keys() for d in all_flattened_rows))
    # Filter out list objects that weren't expanded to keep CSV clean
    fieldnames = sorted([k for k in keys if not isinstance(all_flattened_rows[0].get(k), list)])

    # Write to CSV
    with open(csv_path, 'w', newline='', encoding='utf-8') as output_file:
        dict_writer = csv.DictWriter(output_file, fieldnames=fieldnames, extrasaction='ignore')
        dict_writer.writeheader()
        dict_writer.writerows(all_flattened_rows)

    print(f"Successfully created {csv_path} with {len(fieldnames)} columns.")

if __name__ == "__main__":
    process_avro_to_csv('Test_Tezt.F167x', 'output_report.csv')
