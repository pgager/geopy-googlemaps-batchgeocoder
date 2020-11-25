#!/usr/bin/env python3
import csv
import os
import time
from csv import Dialect

from geopy.exc import (
    GeocoderQueryError,
    GeocoderQuotaExceeded,
    ConfigurationError,
    GeocoderParseError,
    GeocoderTimedOut
)
from geopy.geocoders import GoogleV3

# used to set a google geocoding query by merging this value into one string with comma separated
ADDRESS_COLUMNS_NAME = ["FULL_ADDRESS", "Localidad", "PROV_NAME", "COUNTRY"]
# used to set the locality columns, which can be used to pick the best location in case google returns multiple results
LOCALITY_COLUMN_NAMES = ["Localidad", "PROV_NAME"]
# used to define component restrictions for google geocoding
COMPONENT_RESTRICTIONS_COLUMNS_NAME = {}

# appended columns name to processed data csv
NEW_COLUMNS_NAME = ["Lat", "Long", "Error", "formatted_address", "location_type"]

# delimiter for input csv file
DELIMITER = ","

# Automatically retry X times when GeocoderErrors occur (sometimes the API Service return intermittent failures).
RETRY_COUNTER_CONST = 5

# name for output csv file
INPUT_CSV_FILE = "./tbl1_500.csv"

# name for output csv file
OUTPUT_CSV_FILE = "./updated_tbl1_500.csv"

# google keys - see https://dev.to/gaelsimon/bulk-geocode-addresses-using-google-maps-and-geopy-5bmg for more details
GOOGLE_API_KEY = os.environ.get("GOOGLE_API_KEY")  # it's the new mandatory parameter
if not GOOGLE_API_KEY:
    raise ValueError("Missing environment variable GOOGLE_API_KEY")


# dialect to manage different format of CSV
class CustomDialect(Dialect):
    delimiter = DELIMITER
    quotechar = '"'
    doublequote = True
    skipinitialspace = True
    lineterminator = '\n'
    quoting = csv.QUOTE_ALL


csv.register_dialect('ga', CustomDialect)


def process_addresses_from_csv():
    geo_locator = GoogleV3(api_key=GOOGLE_API_KEY)

    with open(INPUT_CSV_FILE, 'r') as csvinput:
        with open(OUTPUT_CSV_FILE, 'w') as csvoutput:

            # new csv based on same dialect as input csv
            writer = csv.writer(csvoutput, dialect="ga")

            # create a proper header with stripped fieldnames for new CSV
            header = [h.strip('"').strip() for h in next(csvinput).split(DELIMITER)]
            # read Input CSV as Dict of Dict
            reader = csv.DictReader(csvinput, dialect="ga", fieldnames=header)

            # append new columns, to receive geocoded information, to the header of the new CSV
            header = list(reader.fieldnames)
            for column_name in NEW_COLUMNS_NAME:
                header.append(column_name)
            writer.writerow([s.strip() for s in header])

            # iterate through each row of input CSV
            for record in reader:
                # build a line address based on the merge of multiple field values to pass to Google Geocoder
                line_address = ','.join(
                    str(val) for val in (record[column_name] for column_name in ADDRESS_COLUMNS_NAME))

                # if you want to use componentRestrictions feature,
                # build a matching dict {'googleComponentRestrictionField' : 'yourCSVFieldValue'}
                # to pass to Google Geocoder
                component_restrictions = {}
                if COMPONENT_RESTRICTIONS_COLUMNS_NAME:
                    for key, value in COMPONENT_RESTRICTIONS_COLUMNS_NAME.items():
                        component_restrictions[key] = record[value]

                # localities can be use to rank multiple results against each other
                localities = [record[column_name] for column_name in LOCALITY_COLUMN_NAMES]

                # geocode the built line_address, passing optional localities and componentRestrictions
                location = geocode_address(geo_locator, line_address, localities, component_restrictions)

                # build a new temp_row for each csv entry to append to process_data Array
                # first, append existing fieldnames value to this temp_row
                temp_row = [record[column_name] for column_name in reader.fieldnames]
                # then, append geocoded field value to this temp_row
                for column_name in NEW_COLUMNS_NAME:
                    try:
                        temp_row.append(location[column_name])
                    except BaseException as error:
                        print(error)
                        temp_row.append('')

                # Finally append your row with geocoded values with csvwriter.writerow(temp_row)
                try:
                    writer.writerow(temp_row)
                except BaseException as error:
                    print(error)
                    print(temp_row)


def geocode_address(geo_locator, line_address, localities=None, component_restrictions=None, retry_counter=1):
    try:
        # the geopy GoogleV3 geocoding call
        location_results = geo_locator.geocode(line_address, exactly_one=False, components=component_restrictions)

        selected_location = None
        if location_results:
            for location in location_results:
                if not selected_location:  # always set the first result
                    selected_location = location
                else:
                    # check if this location is better
                    if location.raw:
                        address_components = location.raw.get('address_components', [])
                        for address_component in address_components:
                            long_name = address_component.get('long_name', '')
                            short_name = address_component.get('short_name', '')
                            for locality in localities:
                                if locality in [long_name, short_name]:
                                    selected_location = location
                                    break

        if selected_location is not None:
            # build a dict to append to output CSV
            location_result = {"Lat": selected_location.latitude, "Long": selected_location.longitude, "Error": "",
                               "formatted_address": selected_location.raw['formatted_address'],
                               "location_type": selected_location.raw['geometry']['location_type']}
        else:
            location_result = {"Lat": 0, "Long": 0,
                               "Error": "None location found, please verify your address line",
                               "formatted_address": "",
                               "location_type": ""}

    # To catch generic geocoder errors.
    except (ValueError, GeocoderQuotaExceeded, ConfigurationError, GeocoderParseError) as error:
        if hasattr(error, 'message'):
            error_message = error.message
        else:
            error_message = error
        location_result = {"Lat": 0, "Long": 0, "Error": error_message, "formatted_address": "", "location_type": ""}

    # To retry because intermittent failures and timeout sometimes occurs
    except (GeocoderTimedOut, GeocoderQueryError) as geocodingerror:
        if retry_counter < RETRY_COUNTER_CONST:
            return geocode_address(geo_locator, line_address, localities, component_restrictions, retry_counter + 1)
        else:
            if hasattr(geocodingerror, 'message'):
                error_message = geocodingerror.message
            else:
                error_message = geocodingerror
            location_result = {"Lat": 0, "Long": 0, "Error": error_message, "formatted_address": "",
                               "location_type": ""}
    # To retry because intermittent failures and timeout sometimes occurs
    except BaseException as error:
        if retry_counter < RETRY_COUNTER_CONST:
            time.sleep(2)
            return geocode_address(geo_locator, line_address, localities, component_restrictions, retry_counter + 1)
        else:
            location_result = {"Lat": 0, "Long": 0, "Error": error, "formatted_address": "",
                               "location_type": ""}

    print("address line     : {0}".format(line_address))
    print("geocoded address : {0}".format(location_result["formatted_address"]))
    print("location type    : {0}".format(location_result["location_type"]))
    print("Lat/Long         : [{0},{1}]".format(location_result["Lat"], location_result["Long"]))
    print("-------------------")

    return location_result


if __name__ == '__main__':
    process_addresses_from_csv()
