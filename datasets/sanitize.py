#!/usr/bin/env python
import re
import sys

text = sys.stdin.read()
text = re.sub(r'[^\x00-\x7f]', '', text)
text = re.sub(r"([^\s'])\s+'", r"\1'", text)
text = re.sub(r"'\s+([^s'])", r"'\1", text)
print(text)
