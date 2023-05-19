import json
import os
import sys

from xtdb.session import XTDBClient

if __name__ == "__main__":
    client = XTDBClient(os.environ["XTDB_URI"])

    output = client.query(sys.stdin.read())
    sys.stdout.write(json.dumps(output))
