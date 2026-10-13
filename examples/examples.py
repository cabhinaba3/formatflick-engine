import json
from formatflick.formatflick import *
import requests
import os
import glob

"""
This example has been created to demonstrate how we can convert a json file into csv file
The first function create_sample_json_file is just a function that will create a random json file
We dont need to worry about that file as it is not related to the actual formatflick code

the second function demonstrate the usage of the formatflick module
"""


def create_sample_json_file():
    # call the api for json object or use fallback
    print("Call API for JSON object")
    data = None
    try:
        value = requests.get("http://universities.hipolabs.com/search?country=United+States", timeout=5)
        print(f"Status Code: {value.status_code}")
        if value.status_code == 200:
            data = value.json()
    except Exception as e:
        print(f"Network request skipped or failed: {e}. Using local sample.")

    if not data:
        data = [
            {"name": "Marywood University", "country": "United States", "web_pages": ["http://www.marywood.edu"]},
            {"name": "Lindenwood University", "country": "United States", "web_pages": ["http://www.lindenwood.edu/"]}
        ]

    # create the json file
    print("Creating the JSON file...")
    with open("sample.json", "w") as file:
        json.dump(data, file, indent=4)
    print("JSON file creation done...")


def example_1():
    """
    This function initiates the formatflick and converts into from json file to csv file
    """
    create_sample_json_file()
    # convert from json file to csv file
    # initiate the formatflick
    print("Initiating formatflick for JSON")
    core = formatflick(source="sample.json",
                       destination=None,  # change to unset the default file name "result"
                       destination_extension=".csv",  # set if you dont destination is None
                       verbosity=2,  # has no effect till now
                       mode='file'  # by default set to 'file
                       )
    print("Converting...")
    core.convert()
    print("Conversion Done...")


def create_sample_csv():
    print("Call API for CSV file")
    content = None
    try:
        url = 'https://www1.ncdc.noaa.gov/pub/data/cdo/samples/PRECIP_HLY_sample_csv.csv'
        ret = requests.get(url, timeout=5)
        print(f"Status Code: {ret.status_code}")
        if ret.status_code == 200:
            content = ret.content
    except Exception as e:
        print(f"Network request skipped or failed: {e}. Using local sample.")

    if not content:
        content = b"STATION,STATION_NAME,ELEVATION,LATITUDE,LONGITUDE,DATE,HPCP\nCOOP:010008,ABBEVILLE AL US,141.1,31.5702,-85.2482,19480701 01:00,0\n"

    print("Creating the csv file...")
    with open("sample.csv", "wb") as file:
        file.write(content)
    print("CSV file creation done...")


def example_2():
    """
    This function initiates the formatflick and converts from csv to json
    """
    create_sample_csv()
    print("Initiating formatflick for CSV")
    core = formatflick(source="sample.csv",
                       destination=None,  # change to unset the default file name "result"
                       destination_extension=".json",  # set if you dont destination is None
                       verbosity=2,  # has no effect till now
                       mode='file'  # by default set to 'file
                       )
    print("Converting...")
    core.convert()
    print("Conversion Done...")


def example_3():
    create_sample_json_file()
    # convert from json file to csv file
    # initiate the formatflick
    print("Initiating formatflick for JSON")
    core = formatflick(source="sample.json",
                       destination=None,  # change to unset the default file name "result"
                       destination_extension=".xml",  # set if you dont destination is None
                       verbosity=2,  # has no effect till now
                       mode='file'  # by default set to 'file
                       )
    print("Converting...")
    core.convert()
    print("Conversion Done...")


def remove_files():
    for item in [".csv", ".json", ".xml", ".tsv", ".xlsx"]:
        csv_files = glob.glob(f"*{item}")
        for file in csv_files:
            os.remove(file)
            print(f"Deleted: {file}")


if __name__ == "__main__":
    try:
        print("E2E TEST 1:")
        print("-" * 50)
        example_1()
        print("-" * 50)
        print("\nE2E TEST 2:")
        print("-" * 50)
        example_2()
        print("-" * 50)
        print("\nE2E Test 3:")
        print("-" * 50)
        example_3()
        print("-" * 50)

        print("\nCleaning up the files...")
        remove_files()  # occasanly run this function to delete all the junk files
        print("Cleaning Done...Ending...")
    except Exception as err:
        print(f"Some Unknown Error Occurred: {err}")
