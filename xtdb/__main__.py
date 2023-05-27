import json
import os
import sys

from xtdb.session import XTDBClient

if __name__ == "__main__":
    output = XTDBClient(os.environ["XTDB_URI"]).query(sys.stdin.read())
    sys.stdout.write(json.dumps(output))
