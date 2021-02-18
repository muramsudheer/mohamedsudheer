from warcio.archiveiterator import ArchiveIterator
import re
import requests
import sys
import gzip

months = ["January", "February", "March/April", "May/June", "July", "August", "September", "October", "November/December"]
year = ["CC-MAIN-2020-05", "CC-MAIN-2020-10", "CC-MAIN-2020-16", "CC-MAIN-2020-24", "CC-MAIN-2020-29", "CC-MAIN-2020-34", "CC-MAIN-2020-40", "CC-MAIN-2020-45", "CC-MAIN-2020-50"]
keywords = ["COVID", "econ", "affect"]

for i in range(len(year)):
    # wet .gz file for each month of 2020
    monthfiles = "https://commoncrawl.s3.amazonaws.com/crawl-data/" + year[i] + "/wet.paths.gz"
    print("====== " + months[i] + " ======\n")
    # opens .gz file to be read
    with gzip.open(monthfiles,'r') as wetFiles: # Erroring here :( can't seem to figure out how to open the .gz file on a server
        for wetFile in wetFiles:
            if len(sys.argv) > 1:
                wetFile = sys.argv[1]

            stream = None
            if wetFile.startswith("http://") or wetFile.startswith("https://"):
                stream = requests.get(wetFile, stream=True).raw
            else:
                stream = open(wetFile, "rb")

            for record in ArchiveIterator(stream):
                if record.rec_type != "conversion":
                    continue

                contents = (
                    record.content_stream()
                    .read()
                    .decode("utf-8", "replace")
                )
                if keywords[0] in contents and keywords[1] in contents and keywords[2] in contents:
                    url = record.rec_headers.get_header("WARC-Target-URI")
                    print(url + "\n")