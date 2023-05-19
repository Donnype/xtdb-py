import json
import os
import sys

from xtdb.session import XTDBHTTPClient

if __name__ == "__main__":
    client = XTDBHTTPClient(os.environ["XTDB_URI"])

    output = client.query(sys.stdin.read())
    sys.stdout.write(json.dumps(output))
