#!/usr/bin/env python
import re
import sys
import io
import json
import csv

text = sys.stdin.read()
text = re.sub(r'[^\x00-\x7f]', '', text)
text = re.sub(r"([^\s'])\s+'", r"\1'", text)
text = re.sub(r"'\s+([^s'])", r"'\1", text)
rows = [row for row in csv.DictReader(io.StringIO(text), quotechar="'")]
json.dump(rows, sys.stdout, ensure_ascii=False, indent=4)
