import gzip
import fitparse

with gzip.open("C:/Users/jaisi/OneDrive/Desktop/zwift-activity-593784023667267376.fit.gz", 'rb') as f:
    file_content = f.read()

fitfile = fitparse.FitFile(file_content)

for message in fitfile.get_messages():
    if message.name == "record":
        print(message.get_values())
        break

