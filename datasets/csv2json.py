import json
import csv
import sys


rows = [row for row in csv.DictReader(sys.stdin, quotechar="'")]
json.dump(rows, sys.stdout, ensure_ascii=False, indent=4)
