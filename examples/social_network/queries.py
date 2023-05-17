import os
from pprint import pprint

from models import City, Country, User

from xtdb.query import Query
from xtdb.session import XTDBSession

xtdb_session = XTDBSession(os.environ["XTDB_URI"])


print("\nCountry of the user named bA\n")
result = xtdb_session.query(Query(Country).where(City, country=Country).where(User, city=City, name="bA"))
pprint(result)

print("\nCity of the user named bA\n")
result = xtdb_session.query(Query(City).where(City, country=Country).where(User, city=City, name="bA"))
pprint(result)

print("\n2 users in Ireland\n")
result = xtdb_session.query(
    Query(User).where(City, country=Country).where(User, city=City).where(Country, name="Ireland").limit(2)
)
pprint(result)

print("\nAll cities in Ireland\n")
result = xtdb_session.query(Query(City).where(City, country=Country).where(Country, name="Ireland"))
pprint(result)
