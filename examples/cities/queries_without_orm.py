import os
from pprint import pprint

from xtdb.datalog import Find, Where
from xtdb.session import XTDBClient

client = XTDBClient(os.environ["XTDB_URI"])


print("\nCountry of the user named bA\n")
query = Find("(pull Country [*])") & (
    Where("City", "City/country", "Country")
    & Where("Country", "type", '"Country"')
    & Where("User", "User/city", "City")
    & Where("User", "User/name", '"bA"')
)
result = client.query(query)
pprint(result)
