import glob
import gzip
import fitparse

data_files = glob.glob("./data/[0-9]/*.tcx.gz")

for file in data_files:

    with gzip.open(file, 'rb') as f:
        file_content = f.read()

    # fitfile = fitparse.FitFile(file_content)

    # for message in fitfile.get_messages():
    #     if message.name == "record":
    #         print(message.get_values())
    #         print("\n")
    print(file_content)
    print("\n")






















