import avro.datafile
import avro.io

# Path to your Avro file
file_path = "data.avro"

def process_avro_with_standard_pkg(path):
    try:
        with open(path, 'rb') as f:
            reader = avro.datafile.DataFileReader(f, avro.io.DatumReader())
            for record in reader:
                print(f"--- Processing New Record ---")
                
                for key, value in record.items():

                    if isinstance(value, list):
                        print(f"Iterating over list in field '{key}':")
                        for index, item in enumerate(value):
                            print(f"  [{index}]: {item}")
                    

                    elif isinstance(value, dict):
                        print(f"Digging into nested record '{key}':")
                        for sub_key, sub_val in value.items():
                            print(f"  {sub_key}: {sub_val}")
                    
                    else:
                        print(f"{key}: {value}")

            reader.close()

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    process_avro_with_standard_pkg(file_path)
