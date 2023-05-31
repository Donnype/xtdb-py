import os
import random
from pathlib import Path

from models import City, Country, User

from xtdb.session import XTDBSession

countries = [
    "Andorra",
    "Afghanistan",
    "Antigua and Barbuda",
    "Albania",
    "Armenia",
    "Angola",
    "Argentina",
    "Austria",
    "Australia",
    "Azerbaijan",
    "Barbados",
    "Bangladesh",
    "Belgium",
    "Burkina Faso",
    "Bulgaria",
    "Bahrain",
    "Burundi",
    "Benin",
    "Brunei Darussalam",
    "Bolivia",
    "Brazil",
    "Bahamas",
    "Bhutan",
    "Botswana",
    "Belarus",
    "Belize",
    "Canada",
    "Democratic Republic of the Congo",
    "Republic of the Congo",
    "d'Ivoire",
    "Chile",
    "Cameroon",
    "People's Republic of China",
    "Colombia",
    "Costa Rica",
    "Cuba",
    "Cape Verde",
    "Cyprus",
    "Czech Republic",
    "Germany",
    "Djibouti",
    "Denmark",
    "Dominica",
    "Dominican Republic",
    "Ecuador",
    "Estonia",
    "Egypt",
    "Eritrea",
    "Ethiopia",
    "Finland",
    "Fiji",
    "France",
    "Gabon",
    "Georgia",
    "Ghana",
    "The Gambia",
    "Guinea",
    "Greece",
    "Guatemala",
    "Haiti",
    "Guinea-Bissau",
    "Guyana",
    "Honduras",
    "Hungary",
    "Indonesia",
    "Ireland",
    "Israel",
    "India",
    "Iraq",
    "Iran",
    "Iceland",
    "Italy",
    "Jamaica",
    "Jordan",
    "Japan",
    "Kenya",
    "Kyrgyzstan",
    "Kiribati",
    "North Korea",
    "South Korea",
    "Kuwait",
    "Lebanon",
    "Liechtenstein",
    "Liberia",
    "Lesotho",
    "Lithuania",
    "Luxembourg",
    "Latvia",
    "Libya",
    "Madagascar",
    "Marshall Islands",
    "Macedonia",
    "Mali",
    "Myanmar",
    "Mongolia",
    "Mauritania",
    "Malta",
    "Mauritius",
    "Maldives",
    "Malawi",
    "Mexico",
    "Malaysia",
    "Mozambique",
    "Namibia",
    "Niger",
    "Nigeria",
    "Nicaragua",
    "Netherlands",
    "Norway",
    "Nepal",
    "Nauru",
    "New Zealand",
    "Oman",
    "Panama",
    "Peru",
    "Papua New Guinea",
    "Philippines",
    "Pakistan",
    "Poland",
    "Portugal",
    "Palau",
    "Paraguay",
    "Qatar",
    "Romania",
    "Russia",
    "Rwanda",
    "Saudi Arabia",
    "Solomon Islands",
    "Seychelles",
    "Sudan",
    "Sweden",
    "Singapore",
    "Slovenia",
    "Slovakia",
    "Sierra Leone",
    "San Marino",
    "Senegal",
    "Somalia",
    "Suriname",
    "Syria",
    "Togo",
    "Thailand",
    "Tajikistan",
    "Turkmenistan",
    "Tunisia",
    "Tonga",
    "Turkey",
    "Trinidad and Tobago",
    "Tuvalu",
    "Tanzania",
    "Ukraine",
    "Uganda",
    "United States",
    "Uruguay",
    "Uzbekistan",
    "Vatican City",
    "Venezuela",
    "Vietnam",
    "Vanuatu",
    "Yemen",
    "Zambia",
    "Zimbabwe",
    "Algeria",
    "Bosnia and Herzegovina",
    "Cambodia",
    "Central African Republic",
    "Chad",
    "Comoros",
    "Croatia",
    "East Timor",
    "El Salvador",
    "Equatorial Guinea",
    "Grenada",
    "Kazakhstan",
    "Laos",
    "Federated States of Micronesia",
    "Moldova",
    "Monaco",
    "Montenegro",
    "Morocco",
    "Saint Kitts and Nevis",
    "Saint Lucia",
    "Saint Vincent and the Grenadines",
    "Samoa",
    "Serbia",
    "South Africa",
    "Spain",
    "Sri Lanka",
    "Swaziland",
    "Switzerland",
    "United Arab Emirates",
    "United Kingdom",
]


def main():
    cities = [
        [city.split(",")[0], int(city.split(",")[1]), city.split(",")[2]]
        for city in (Path() / "cities.csv").read_text().splitlines()[1:]
    ]

    country_map = {}
    xtdb_session = XTDBSession(os.environ["XTDB_URI"])

    with xtdb_session:
        for country in countries:
            country_entity = Country(name=country)
            xtdb_session.put(country_entity)
            country_map[country] = country_entity

    city_map = {}

    with xtdb_session:
        for name, population, country_name in cities:
            city_entity = City(name=name, population=population, country=country_map[str(country_name)])
            xtdb_session.put(city_entity)
            city_map[name] = city_entity

    alfabet = "abcdefghijklmnopqrstuvwxyz"
    alfabet += alfabet.upper()

    with xtdb_session:
        for x in alfabet:
            for y in alfabet:
                city = list(city_map.values())[random.randint(0, len(city_map) - 1)]
                xtdb_session.put(User(name=x + y, city=city, country=city.country))


if __name__ == "__main__":
    main()
