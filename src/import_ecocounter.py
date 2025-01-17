import json
import requests
import datetime
import pytz
import math
import os
import time
from bearer_auth import BearerAuth
from apscheduler.schedulers.blocking import BlockingScheduler

# API URLs
API_BASE_URL = os.environ.get('API_URL')
API_TOKEN = API_BASE_URL + "/token"
API_URL = API_BASE_URL + "/api/1.0"
API_SITES = API_URL + "/site"
API_DATA = API_URL + "/data/site/"

bearerToken = {'token': None, 'timeout': None}

API_KEY = os.environ.get('API_KEY')
API_SECRET = os.environ.get('API_SECRET')
api_auth = (API_KEY, API_SECRET)

# FROST URLs
FROST_BASE_URL = os.environ.get('FROST_SERVER')
FROST_THINGS_WITH_DATASTREAMS = FROST_BASE_URL+"/Things?$expand=Datastreams,Locations"
FROST_OBSERVATIONS = FROST_BASE_URL+"/Datastreams(<DATASTREAM_ID>)/Observations?$filter=not phenomenonTime lt <STARTTIME>"
POST_URL = FROST_BASE_URL+"/CreateObservations"

FROST_USER = os.environ.get('FROST_USER')
FROST_PASS = os.environ.get('FROST_PASSWORD')
frost_auth=(FROST_USER,FROST_PASS)

# Interval definitions
INTERVAL_15_MIN = "15-Min"
INTERVAL_1_HOUR = "1-Stunde"
INTERVAL_1_DAY = "1-Tag"
INTERVAL_1_WEEK = "1-Woche"
INTERVAL_1_MONTH = "1-Monat"
INTERVAL_1_YEAR = "1-Jahr"

INTERVAL_15_MIN_STEP = "15m"
INTERVAL_1_HOUR_STEP = "hour"
INTERVAL_1_DAY_STEP = "day"
INTERVAL_1_WEEK_STEP = "week"
INTERVAL_1_MONTH_STEP = "month"
INTERVAL_1_YEAR_STEP = "year"


INTERVAL_15_MIN_LABEL = "15 Minuten"
INTERVAL_1_HOUR_LABEL = "Stunde"
INTERVAL_1_DAY_LABEL = "Tag"
INTERVAL_1_WEEK_LABEL = "Woche"
INTERVAL_1_MONTH_LABEL = "Monat"
INTERVAL_1_YEAR_LABEL = "Jahr"

INTERVAL_15_MIN_DURATION = datetime.timedelta(minutes=15)
INTERVAL_1_HOUR_DURATION = datetime.timedelta(hours=1)
INTERVAL_1_DAY_DURATION = datetime.timedelta(days=1)
INTERVAL_1_WEEK_DURATION = datetime.timedelta(days=7)

TIMEZONE = pytz.timezone("Europe/Berlin")
UTC = pytz.utc

sites =  [
    {'id': 100024661, 'name': 'Jannowitzbrücke', 'location': 'Jannowitzbrücke', 'direction': 'Beide', 'district': 'Mitte'},
    {'id': 100032152, 'name': 'Invalidenstraße', 'location': 'Invalidenstraße', 'direction': 'Beide', 'district': 'Mitte'},
    {'id': 300023833, 'name': 'Oberbaumbrücke', 'location': 'Oberbaumbrücke', 'direction': 'Beide', 'district': 'Friedrichshain-Kreuzberg'},
    {'id': 100032154, 'name': 'Frankfurter Allee', 'location': 'Frankfurter Allee', 'direction': 'Beide', 'district': 'Friedrichshain-Kreuzberg'},
    {'id': 300019043, 'name': 'Straße des 17. Juni', 'location': 'Straße des 17. Juni', 'direction': 'Beide', 'district': 'Charlottenburg-Wilmersdorf'},
    {'id': 100032155, 'name': 'Berliner Straße', 'location': 'Berliner Straße', 'direction': 'Beide', 'district': 'Pankow'},
    {'id': 100032159, 'name': 'Schwedter Steg', 'location': 'Schwedter Steg', 'direction': 'Beide', 'district': 'Pankow'},
    {'id': 100033039, 'name': 'Prinzregentenstraße', 'location': 'Prinzregentenstraße', 'direction': 'Beide', 'district': 'Charlottenburg-Wilmersdorf'},
    {'id': 100032161, 'name': 'Klosterstraße', 'location': 'Klosterstraße', 'direction': 'Beide', 'district': 'Spandau'},
    {'id': 100032163, 'name': 'Breitenbachplatz', 'location': 'Breitenbachplatz', 'direction': 'Beide', 'district': 'Steglitz-Zehlendorf'},
    {'id': 100033038, 'name': 'Yorckstraße', 'location': 'Yorckstraße', 'direction': 'Beide', 'district': 'Tempelhof-Schöneberg'},
    {'id': 100032165, 'name': 'Monumentenstraße', 'location': 'Monumentenstraße', 'direction': 'Beide', 'district': 'Tempelhof-Schöneberg'},
    {'id': 100064714, 'name': 'Mariendorfer Damm', 'location': 'Mariendorfer Damm', 'direction': 'Beide', 'district': 'Tempelhof-Schöneberg'},
    {'id': 100032236, 'name': 'Maybachufer', 'location': 'Maybachufer', 'direction': 'Beide', 'district': 'Neukölln'},
    {'id': 100032237, 'name': 'Kaisersteg', 'location': 'Kaisersteg', 'direction': 'Beide', 'district': 'Treptow-Köpenick'},
    {'id': 100033037, 'name': 'Alberichstraße', 'location': 'Alberichstraße', 'direction': 'Beide', 'district': 'Marzahn-Hellersdorf'},
    {'id': 100033035, 'name': 'Paul- und Paula-Uferweg', 'location': 'Paul-und-Paula-Uferweg', 'direction': 'Beide', 'district': 'Lichtenberg'},
    {'id': 100032169, 'name': 'Markstraße', 'location': 'Markstraße', 'direction': 'Beide', 'district': 'Reinickendorf'},
    {'id': 101024661, 'name': 'Jannowitzbrücke', 'location': 'Jannowitzbrücke', 'direction': 'Alexanderplatz', 'district': 'Mitte'},
    {'id': 102024661, 'name': 'Jannowitzbrücke', 'location': 'Jannowitzbrücke', 'direction': 'Moritzplatz', 'district': 'Mitte'},
    {'id': 101032152, 'name': 'Invalidenstraße', 'location': 'Invalidenstraße', 'direction': 'Osten', 'district': 'Mitte'},
    {'id': 102032152, 'name': 'Invalidenstraße', 'location': 'Invalidenstraße', 'direction': 'Westen', 'district': 'Mitte'},
    {'id': 101032154, 'name': 'Frankfurter Allee', 'location': 'Frankfurter Allee', 'direction': 'Lichtenberg', 'district': 'Friedrichshain-Kreuzberg'},
    {'id': 102032154, 'name': 'Frankfurter Allee', 'location': 'Frankfurter Allee', 'direction': 'Alexanderplatz', 'district': 'Friedrichshain-Kreuzberg'},
    {'id': 101032155, 'name': 'Berliner Straße', 'location': 'Berliner Straße', 'direction': 'Norden', 'district': 'Pankow'},
    {'id': 102032155, 'name': 'Berliner Straße', 'location': 'Berliner Straße', 'direction': 'Süden', 'district': 'Pankow'},
    {'id': 101032159, 'name': 'Schwedter Steg', 'location': 'Schwedter Steg', 'direction': 'Norden', 'district': 'Pankow'},
    {'id': 102032159, 'name': 'Schwedter Steg', 'location': 'Schwedter Steg', 'direction': 'Süden', 'district': 'Pankow'},
    {'id': 101032161, 'name': 'Klosterstraße', 'location': 'Klosterstraße', 'direction': 'Norden', 'district': 'Spandau'},
    {'id': 102032161, 'name': 'Klosterstraße', 'location': 'Klosterstraße', 'direction': 'Süden', 'district': 'Spandau'},
    {'id': 101032163, 'name': 'Breitenbachplatz', 'location': 'Breitenbachplatz', 'direction': 'Osten', 'district': 'Steglitz-Zehlendorf'},
    {'id': 102032163, 'name': 'Breitenbachplatz', 'location': 'Breitenbachplatz', 'direction': 'Westen', 'district': 'Steglitz-Zehlendorf'},
    {'id': 101032165, 'name': 'Monumentenstraße', 'location': 'Monumentenstraße', 'direction': 'Osten', 'district': 'Tempelhof-Schöneberg'},
    {'id': 102032165, 'name': 'Monumentenstraße', 'location': 'Monumentenstraße', 'direction': 'Westen', 'district': 'Tempelhof-Schöneberg'},
    {'id': 101032169, 'name': 'Markstraße', 'location': 'Markstraße', 'direction': 'Süden', 'district': 'Reinickendorf'},
    {'id': 102032169, 'name': 'Markstraße', 'location': 'Markstraße', 'direction': 'Norden', 'district': 'Reinickendorf'},
    {'id': 101032236, 'name': 'Maybachufer', 'location': 'Maybachufer', 'direction': 'Osten', 'district': 'Neukölln'},
    {'id': 102032236, 'name': 'Maybachufer', 'location': 'Maybachufer', 'direction': 'Westen', 'district': 'Neukölln'},
    {'id': 101032237, 'name': 'Kaisersteg', 'location': 'Kaisersteg', 'direction': 'Norden', 'district': 'Treptow-Köpenick'},
    {'id': 102032237, 'name': 'Kaisersteg', 'location': 'Kaisersteg', 'direction': 'Süden', 'district': 'Treptow-Köpenick'},
    {'id': 101033035, 'name': 'Paul- und Paula-Uferweg', 'location': 'Paul-und-Paula-Uferweg', 'direction': 'Westen', 'district': 'Lichtenberg'},
    {'id': 102033035, 'name': 'Paul- und Paula-Uferweg', 'location': 'Paul-und-Paula-Uferweg', 'direction': 'Osten', 'district': 'Lichtenberg'},
    {'id': 101033037, 'name': 'Alberichstraße', 'location': 'Alberichstraße', 'direction': 'Süden', 'district': 'Marzahn-Hellersdorf'},
    {'id': 102033037, 'name': 'Alberichstraße', 'location': 'Alberichstraße', 'direction': 'Norden', 'district': 'Marzahn-Hellersdorf'},
    {'id': 101033038, 'name': 'Yorckstraße', 'location': 'Yorckstraße', 'direction': 'Kreuzberg', 'district': 'Tempelhof-Schöneberg'},
    {'id': 102033038, 'name': 'Yorckstraße', 'location': 'Yorckstraße', 'direction': 'Schöneberg', 'district': 'Tempelhof-Schöneberg'},
    {'id': 101033039, 'name': 'Prinzregentenstraße', 'location': 'Prinzregentenstraße', 'direction': 'Norden', 'district': 'Charlottenburg-Wilmersdorf'},
    {'id': 102033039, 'name': 'Prinzregentenstraße', 'location': 'Prinzregentenstraße', 'direction': 'Süden', 'district': 'Charlottenburg-Wilmersdorf'},
    {'id': 353310274, 'name': 'Oberbaumbrücke', 'location': 'Oberbaumbrücke', 'direction': 'Friedrichshain', 'district': 'Friedrichshain-Kreuzberg'},
    {'id': 353310273, 'name': 'Oberbaumbrücke', 'location': 'Oberbaumbrücke', 'direction': 'Kreuzberg', 'district': 'Friedrichshain-Kreuzberg'},
    {'id': 101064714, 'name': 'Mariendorfer Damm', 'location': 'Mariendorfer Damm', 'direction': 'Norden', 'district': 'Tempelhof-Schöneberg'},
    {'id': 102064714, 'name': 'Mariendorfer Damm', 'location': 'Mariendorfer Damm', 'direction': 'Süden', 'district': 'Tempelhof-Schöneberg'},
    {'id': 353277807, 'name': 'Straße des 17. Juni', 'location': 'Straße des 17. Juni', 'direction': 'Charlottenburg', 'district': 'Charlottenburg-Wilmersdorf'},
    {'id': 353277806, 'name': 'Straße des 17. Juni', 'location': 'Straße des 17. Juni', 'direction': 'Mitte', 'district': 'Charlottenburg-Wilmersdorf'},
    {'id': 300021646, 'name': 'Karl-Marx-Allee', 'location': 'Karl-Marx-Allee', 'direction': 'Westen', 'district': 'Mitte'},
    {'id': 300028062, 'name': 'Senefelderplatz', 'location': 'Senefelderplatz', 'direction': 'Norden', 'district': 'Pankow'},
    {'id': 300037797, 'name': 'Nordufer', 'location': 'Nordufer', 'direction': 'Beide', 'district': 'Mitte'},
    {'id': 353412943, 'name': 'Nordufer', 'location': 'Nordufer', 'direction': 'Osten', 'district': 'Mitte'},
    {'id': 353412944, 'name': 'Nordufer', 'location': 'Nordufer', 'direction': 'Westen', 'district': 'Mitte'},
    {'id': 300037798, 'name': 'Nonnendammallee', 'location': 'Nonnendammallee', 'direction': 'Beide', 'district': 'Spandau'},
    {'id': 353412946, 'name': 'Nonnendammallee', 'location': 'Nonnendammallee', 'direction': 'Osten', 'district': 'Spandau'},
    {'id': 353412947, 'name': 'Nonnendammallee', 'location': 'Nonnendammallee', 'direction': 'Westen', 'district': 'Spandau'},
    {'id': 300041564, 'name': 'Strausberger Platz', 'location': 'Strausberger Platz', 'direction': 'Osten', 'district': 'Mitte'},
    {'id': 300041566, 'name': 'Schönhauser Allee', 'location': 'Schönhauser Allee', 'direction': 'Süden', 'district': 'Pankow'},
]

observedProperty = None
sensor = None
things = None

sched = BlockingScheduler()

def getToken():
    global bearerToken

    if bearerToken['token'] is None  or bearerToken['timeout'] is None or bearerToken['timeout'] <= datetime.datetime.now():
        response = requests.post(API_TOKEN, data = {'grant_type': 'client_credentials'}, auth=api_auth)
        if (response.status_code == 200):
            json_response = response.json()
            print(json.dumps(json_response, indent=4, sort_keys=False))
            bearerToken['token'] = json_response['access_token']
            bearerToken['timeout'] = datetime.datetime.now() + datetime.timedelta(seconds=json_response['expires_in'])
        else:
            print("Error "+str(response.status_code))
            print(response.text)
    return BearerAuth(bearerToken['token'])

def get_siteDetails(site):
    for siteDetail in sites:
        if site['id'] == siteDetail['id']:
            return siteDetail
    return None


def init():
    init_observedProperty()
    init_sensor()
    init_things()

def init_observedProperty():
    global observedProperty
    # load measurements
    observedProperty = load_observedProperty()

    if observedProperty is None:
        # create measurement
        observedProperty = create_observedProperty()

def load_observedProperty():
    q_res = requests.get(FROST_BASE_URL+'/ObservedProperties', auth=frost_auth)
    if (q_res.status_code == 200):
        json_response = q_res.json()
        if 'value' in json_response:
            for property in  json_response['value']:
                if 'name' in property and property['name'] == 'Verkehrsstärke':
                    return property['@iot.id']
    else:
        print("Error "+str(response.status_code))
        print(response.text)
        raise Exception('Could not load ObservedProperty!')
    return None

def create_observedProperty():
    q_data = {
        "name": "Verkehrsstärke",
        "description": "SenUVK Definition",
        "definition": "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement"
    }

    q_res = requests.post(FROST_BASE_URL+'/ObservedProperties', auth=frost_auth, json=q_data)
    if (q_res.status_code != 201):
        print("Could not create ObservedProperty for 'Verkehrsstärke'")
    else:
        q_data = requests.get(q_res.headers['location'])
        print('"Verkehrsstärke"-id: ' + str(q_data.json()['@iot.id']))
        return q_data.json()['@iot.id']

def init_sensor():
    global sensor
    # load sensor
    sensor = load_sensor()

    if sensor is None:
        # create sensor
        observedProperty = create_sensor()

def load_sensor():
    q_res = requests.get(FROST_BASE_URL+'/Sensors', auth=frost_auth)
    if (q_res.status_code == 200):
        json_response = q_res.json()
        if 'value' in json_response:
            for sensor in  json_response['value']:
                if 'name' in sensor and sensor['name'] == 'EcoCounter':
                    return sensor['@iot.id']
    else:
        print("Error "+str(response.status_code))
        print(response.text)
        raise Exception('Could not load Sensor!')
    return None

def create_sensor():
    sensor_data = {
        "name" : "EcoCounter",
        "description" : "Fahrradzählstellen Eco-Counter",
        "encodingType" : "text/html",
        "metadata" : "https://de.eco-counter.com/"
    }

    q_res = requests.post(FROST_BASE_URL+'/Sensors', auth=frost_auth, json=sensor_data)
    if (q_res.status_code != 201):
        print("Could not create Sensor for 'EcoCounter'")
    else:
        q_data = requests.get(q_res.headers['location'])
        print('Sensor-id: ' + str(q_data.json()['@iot.id']))
        return q_data.json()['@iot.id']

def init_things():
    global things
    # load things
    things = load_things()
    # load sites
    sites = get_sites()
    # Update
    update_things(things, sites)
    # Reload changed things
    things = load_things()

def update_things(things, sites):
    for site in sites:
        thing = find_thing(things, site)
        if thing is None:
            create_thing(site)
        else:
            update_thing(thing, site)

def find_thing(things, site):
    for thing in things:
        if thing['properties']['siteID'] == site['id']:
            return thing
    return None

def create_thing(site):
    siteDetails = get_siteDetails(site)
    if not siteDetails is None:
        thing = {
            "name" : str(site['id']),
            "description" : "Fahrradzählstelle "+siteDetails['location']+" ("+siteDetails['district']+")",
            "properties" : {
                "siteID": site['id'],
                "siteName": site['name'],
                "status": None,
                "location": siteDetails['location'],
                "district": siteDetails['district'],
                "userType": site['userType'],
                "installationDate": datetime.datetime.strptime(site['installationDate'], "%Y-%m-%dT%H:%M:%S%z").strftime("%d.%m.%Y"),
                "firstData": site['firstData'],
                "photos": site['photos'],
            },
            "Locations": [
                {
                    "name": siteDetails['location'],
                    "description": siteDetails['location']+"("+siteDetails['district']+")",
                    "encodingType": "application/geo+json",
                    "location": {
                        "type": "Point",
                        "coordinates": [site['longitude'], site['latitude']]
                    }
                }
            ],
            "Datastreams": []
        }
        create_datastreams(thing, site);
        if 'channels' in site and site['channels'] is not None:
            for channel in site['channels']:
                create_datastreams(thing, channel);

        # Store Thing in Frost-Server
        print(json.dumps(thing, indent=4, sort_keys=True))
        q_res = requests.post(FROST_BASE_URL+'/Things', auth=frost_auth, json=thing)
        if (q_res.status_code != 201):
            print("Could not create Thing "+thing['name'])
            print(q_res.text)
        else:
            print("Created Thing "+thing['name'])

def create_datastreams(thing, site):
    siteDetails = get_siteDetails(site)
    if siteDetails is None:
        print("Could not find Details for Site: "+str(site['id']))
        return None
    thing['Datastreams'].append(create_datastream(site, INTERVAL_15_MIN_STEP, INTERVAL_15_MIN, INTERVAL_15_MIN_LABEL))
    thing['Datastreams'].append(create_datastream(site, INTERVAL_1_HOUR_STEP, INTERVAL_1_HOUR, INTERVAL_1_HOUR_LABEL))
    thing['Datastreams'].append(create_datastream(site, INTERVAL_1_DAY_STEP, INTERVAL_1_DAY, INTERVAL_1_DAY_LABEL))
    thing['Datastreams'].append(create_datastream(site, INTERVAL_1_WEEK_STEP, INTERVAL_1_WEEK, INTERVAL_1_WEEK_LABEL))
    thing['Datastreams'].append(create_datastream(site, INTERVAL_1_MONTH_STEP, INTERVAL_1_MONTH, INTERVAL_1_MONTH_LABEL))
    thing['Datastreams'].append(create_datastream(site, INTERVAL_1_YEAR_STEP, INTERVAL_1_YEAR, INTERVAL_1_YEAR_LABEL))

def create_datastream(site, step, step_name_part, step_label):
    siteDetails = get_siteDetails(site)
    return {
            "name": "Anzahl Fahrräder "+step_label + " - Richtung: "+siteDetails['direction'],
            "description" : "Anzahl Fahrräder pro "+step_label+" für Site: "+str(site['id']) + " - Richtung: "+siteDetails['direction'],
            "observationType": "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement",
            "Sensor": {"@iot.id": sensor},
            "unitOfMeasurement": {
                "name": "Verkehrsstärke",
                "symbol": "Fahrräder/"+step_label
            },
            "ObservedProperty": {"@iot.id": observedProperty},
            "properties": {
                "siteID": site['id'],
                "layerName": "Anzahl_Fahrrad_Zaehlstelle_"+step_name_part,
                "step": step,
                "periodLength": step_name_part,
                "periodLengthLabel": step_label,
                "direction": siteDetails["direction"]
            }
        }

def update_thing(thing, site):
    updatedThing = {'properties':thing['properties']}
    siteDetails = get_siteDetails(site)
    changed = False
    if thing['properties']['siteID'] != site['id']:
        updatedThing['properties']['siteID'] = site['id']
        changed = True
    if thing['name'] != str(site['id']):
        updatedThing['name'] =  str(site['id'])
        updatedThing['description'] = "Fahrradzählstelle "+siteDetails['location']+" ("+siteDetails['district']+")"
        updatedThing['properties']['siteName'] = site['name']
        changed = True
    if thing['properties']['userType'] != site['userType']:
        updatedThing['properties']['userType'] = site['userType']
        changed = True
    if thing['properties']['installationDate'] != datetime.datetime.strptime(site['installationDate'], "%Y-%m-%dT%H:%M:%S%z").strftime("%d.%m.%Y"):
        updatedThing['properties']['installationDate'] = datetime.datetime.strptime(site['installationDate'], "%Y-%m-%dT%H:%M:%S%z").strftime("%d.%m.%Y");
        changed = True
    if thing['properties']['firstData'] != site['firstData']:
        updatedThing['properties']['firstData'] = site['firstData']
        changed = True
    if ('photos' in thing['properties']) and ('photos' in site):
        if thing['properties']['photos'] != site['photos']:
            updatedThing['properties']['photos'] = site['photos']
            changed = True
    elif ('photos' in thing['properties']) or ('photos' in site):
        updatedThing['properties']['photos'] = site['photos']
        changed = True
    if not siteDetails is None:
        if thing['properties']['location'] != siteDetails['location']:
            updatedThing['properties']['location'] = siteDetails['location']
            updatedThing['description'] = "Fahrradzählstelle "+siteDetails['location']+" ("+siteDetails['district']+")"
            changed = True
        if thing['properties']['district'] != siteDetails['district']:
            updatedThing['properties']['district'] = siteDetails['district']
            updatedThing['description'] = "Fahrradzählstelle "+siteDetails['location']+" ("+siteDetails['district']+")"
            changed = True

    if changed:
        # Update Thing in Frost-Server
        q_res = requests.patch(FROST_BASE_URL+'/Things('+str(thing['@iot.id'])+')', auth=frost_auth, json=updatedThing)
        if (q_res.status_code != 200):
            print(json.dumps(updatedThing, indent=4, sort_keys=True))
            print("Could not update Thing "+thing['name']+'('+str(thing['@iot.id'])+')')
            print(q_res.text)
        else:
            print("Updated Thing "+thing['name']+'('+str(thing['@iot.id'])+')')

    updateDatastreams(thing, site)

def updateDatastreams(thing, site):
    siteIDs = []
    for datastream in thing["Datastreams"]:
        datastreamSite = findSite(datastream["properties"]["siteID"], site)
        siteIDs.append(datastream["properties"]["siteID"])
        if not datastreamSite is None:
            updatedDatastream = {'properties':datastream['properties']}
            siteDetails = get_siteDetails(datastreamSite)
            changed = False


            if datastream["name"] != "Anzahl Fahrräder "+datastream["properties"]["periodLengthLabel"] + " - Richtung: "+siteDetails['direction']:
                updatedDatastream["name"] = "Anzahl Fahrräder "+datastream["properties"]["periodLengthLabel"] + " - Richtung: "+siteDetails['direction']
                changed = True
            if datastream["description"] != "Anzahl Fahrräder pro "+datastream["properties"]["periodLengthLabel"]+" für Site: "+str(datastreamSite['id']) + " - Richtung: "+siteDetails['direction']:
                updatedDatastream["description"] = "Anzahl Fahrräder pro "+datastream["properties"]["periodLengthLabel"]+" für Site: "+str(datastreamSite['id']) + " - Richtung: "+siteDetails['direction']
                changed = True
            if datastream["properties"]["direction"] != siteDetails["direction"]:
                updatedDatastream["properties"]["direction"] = siteDetails["direction"]
                changed = True

            if changed:
            # Update Datastream in Frost-Server
                q_res = requests.patch(FROST_BASE_URL+'/Datastreams('+str(datastream['@iot.id'])+')', auth=frost_auth, json=updatedDatastream)
                if (q_res.status_code != 200):
                    print(json.dumps(updatedDatastream, indent=4, sort_keys=True))
                    print("Could not update Datastream "+datastream['name']+'('+str(datastream['@iot.id'])+')')
                    print(q_res.text)
                else:
                    print("Updated Datastream "+updatedDatastream['name']+'('+str(datastream['@iot.id'])+')')

    newDatastreams = []
    if 'channels' in site and site['channels'] is not None:
        for channel in site['channels']:
            if not channel['id'] in siteIDs:
                siteDetails = get_siteDetails(channel)
                if not siteDetails is None:
                    newDatastreams.append(create_datastream(channel, INTERVAL_15_MIN_STEP, INTERVAL_15_MIN, INTERVAL_15_MIN_LABEL))
                    newDatastreams.append(create_datastream(channel, INTERVAL_1_HOUR_STEP, INTERVAL_1_HOUR, INTERVAL_1_HOUR_LABEL))
                    newDatastreams.append(create_datastream(channel, INTERVAL_1_DAY_STEP, INTERVAL_1_DAY, INTERVAL_1_DAY_LABEL))
                    newDatastreams.append(create_datastream(channel, INTERVAL_1_WEEK_STEP, INTERVAL_1_WEEK, INTERVAL_1_WEEK_LABEL))
                    newDatastreams.append(create_datastream(channel, INTERVAL_1_MONTH_STEP, INTERVAL_1_MONTH, INTERVAL_1_MONTH_LABEL))
                    newDatastreams.append(create_datastream(channel, INTERVAL_1_YEAR_STEP, INTERVAL_1_YEAR, INTERVAL_1_YEAR_LABEL))

    for datastream in newDatastreams:
        r = requests.post(url=FROST_BASE_URL+"/Things("+str(thing['@iot.id'])+")/Datastreams", auth=frost_auth, json=datastream)
        if (r.status_code == 201):
            print("Datastream created for Thing "+str(thing['@iot.id'])+": " + datastream["name"])
            r = requests.get(r.headers["location"])
            if (r.status_code == 200):
                datastream['@iot.id'] = r.json()['@iot.id']
        else:
            print("Could not create Datastream for Thing "+str(thing['@iot.id'])+": " + datastream["name"])
            print(str(r.status_code)+": "+r.json()['message'])




def findSite(siteID, site):
    if site['id'] == siteID:
        return site
    if 'channels' in site and site['channels'] is not None:
        for channel in site['channels']:
            if channel['id'] == siteID:
                return channel
    return None


def get_sites():
    json_response = []

    response = requests.get(API_SITES, auth=getToken())
    if (response.status_code == 200):
        json_response = response.json()
    else:
        print("Error "+str(response.status_code))

    return json_response

def load_observations(datastream, starttime):
    url = FROST_OBSERVATIONS.replace('<DATASTREAM_ID>', str(datastream['@iot.id'])).replace('<STARTTIME>', starttime.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ"))
    results = []
    r = requests.get(url)
    if (r.status_code == 200):
        json_response = r.json()
        if 'value' in json_response:
            results += json_response['value']
        else:
            results.append(json_response)
        while '@iot.nextLink' in json_response:
            r = requests.get(json_response['@iot.nextLink'])
            if r.status_code != 200:
                print(json_response['@iot.nextLink'])
                print(r.text)
                print(str(r.status_code)+": "+r.json()['message'])
                raise Exception("Could not load Data from Frost")
            json_response = r.json()
            results += json_response['value']
    else:
        print(url)
        print(r.text)
        raise Exception("Could not load Data from Frost")
    observations = {}
    duplicates = []
    for result in results:
        phenomenonTime = result['phenomenonTime']
        phenomenonTimeSplit = phenomenonTime.split('/')
        phenomenonTimeStart = UTC.localize(datetime.datetime.strptime(phenomenonTimeSplit[0], "%Y-%m-%dT%H:%M:%SZ"))
        phenomenonTimeEnd = UTC.localize(datetime.datetime.strptime(phenomenonTimeSplit[1], "%Y-%m-%dT%H:%M:%SZ"))
        phenomenonTime = phenomenonTimeStart.strftime("%Y-%m-%dT%H:%M:%SZ")+'/'+phenomenonTimeEnd.strftime("%Y-%m-%dT%H:%M:%SZ")
        if phenomenonTime in observations:
            duplicates.append(result)
        else:
            observations[phenomenonTime] = result
    for duplicate in duplicates:
        print("Delete Duplicate Observation: "+str(duplicate["@iot.id"]))
        r = requests.delete(FROST_BASE_URL+"/Observations("+str(duplicate["@iot.id"])+")", auth=frost_auth)
        if (r.status_code != 200 and r.status_code != 201):
            print("Could not delete Observations")
            print(str(r.status_code)+": "+r.text)

    return observations

def import_observations():

    for thing in things:
        for datastream in thing['Datastreams']:
            try:
                importForDatastream(thing, datastream)
            except:
                print("Retry after 5 seconds!")
                time.sleep(5)
                try:
                    importForDatastream(thing, datastream)
                except Exception as e:
                    print("Import failed!")
                    print(e)


def importForDatastream(thing, datastream):
    start = TIMEZONE.localize(datetime.datetime.now() - datetime.timedelta(days=7))
    # start = TIMEZONE.localize(datetime.datetime(2015, 1, 1))
    observations = []
    print(
        'Site: ' + str(datastream['properties']['siteID']) + ' (' + thing['properties']['location'] + ' - Richtung: ' +
        datastream['properties']['direction'] + ')')
    begin = startOfStep(start, datastream['properties']['step'])
    siteDetails = get_siteDetails({'id': datastream['properties']['siteID']})
    if not siteDetails is None and 'importFrom' in siteDetails: # and not datastream['properties']['step'] == INTERVAL_15_MIN_STEP and not datastream['properties']['step'] == INTERVAL_1_HOUR_STEP:
        begin = startOfStep(TIMEZONE.localize(datetime.datetime.strptime(siteDetails['importFrom'], "%Y-%m-%d")), datastream['properties']['step'])
    params = {'begin': (begin).strftime("%Y-%m-%dT%H:%M:%S"), 'step': datastream['properties']['step'], 'complete': 'false'}
    print(params)
    response = requests.get(API_DATA + str(datastream['properties']['siteID']), params=params, auth=getToken())
    if (response.status_code == 401):
        global bearerToken
        bearerToken['token'] = None
        response = requests.get(API_DATA + str(datastream['properties']['siteID']), params=params, auth=getToken())
    if (response.status_code == 200):
        existing_observations = load_observations(datastream, begin)
        json_response = response.json()
        print('Results: ' + str(len(json_response)))
        if (datastream['properties']['step'] == 'day'):
            status = 'active' if len(json_response) > 0 else 'inactive'
            if not 'status' in thing['properties'] or not thing['properties']['status'] == status:
                set_thing_status(thing, status)
        for result in json_response:
            observation = create_or_update_observation(result, datastream, existing_observations)
            if not observation is None:
                observations.append(observation)
    else:
        print(response.status_code)
    if len(observations) > 0:
        post_observations(observations)
        observations = []


def set_thing_status(thing, status):
    updatedThing = {'properties':thing['properties']}
    updatedThing['properties']['status'] = status

    # Update Thing-Status in Frost-Server
    q_res = requests.patch(FROST_BASE_URL+'/Things('+str(thing['@iot.id'])+')', auth=frost_auth, json=updatedThing)
    if (q_res.status_code != 200):
        print(json.dumps(thing, indent=4, sort_keys=True))
        print("Could not update Thing "+thing['name']+'('+str(thing['@iot.id'])+')')
        print(q_res.text)
    else:
        print("Set Thing "+thing['name']+'('+str(thing['@iot.id'])+') to '+status)

def post_observations(observations):
    print('Observations: '+str(len(observations)))
    while len(observations) > 1000:
        firstObservations = observations[:1000]
        observations = observations[1000:]
        r = requests.post(url=POST_URL, auth=frost_auth, json=firstObservations, headers={"Content-Type": "application/json;charset=UTF-8"})
        if (r.status_code != 200 and r.status_code != 201):
            print("Could not save Observations")
            print(str(r.status_code)+": "+r.text)
    if len(observations) > 0:
        r = requests.post(url=POST_URL, auth=frost_auth, json=observations, headers={"Content-Type": "application/json;charset=UTF-8"})
        if (r.status_code != 200 and r.status_code != 201):
            print("Could not save Observations")
            print(str(r.status_code)+": "+r.text)

def update_obersvation(observation):
    q_res = requests.patch(FROST_BASE_URL+'/Observations('+str(observation['@iot.id'])+')', auth=frost_auth, json=observation)
    if (q_res.status_code != 200):
        print("Could not update Observation "+observation['phenomenonTime']+'('+str(observation['@iot.id'])+')')
        print(q_res.text)
    else:
        print("Updated Observation "+observation['phenomenonTime']+'('+str(observation['@iot.id'])+')')


def create_or_update_observation(result, datastream, observations):
    isoDateStart = datetime.datetime.strptime(result['isoDate'], '%Y-%m-%dT%H:%M:%S%z')
    isoDateStart = TIMEZONE.localize(isoDateStart.replace(tzinfo=None))
    isoDateEnd = getEndTime(isoDateStart, datastream['properties']['step'])
    phenomenonTime = isoDateStart.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")+'/'+isoDateEnd.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    if phenomenonTime in observations:
        print('Found existing Observation: '+phenomenonTime)
        observation = observations[phenomenonTime]
        if not observation['result'] == result['counts']:
            observation['result'] = result['counts']
            observation = {
                "@iot.id" : observation['@iot.id'],
                "phenomenonTime": observation['phenomenonTime'],
                "resultTime": datetime.datetime.now().astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "result": observation['result']
            }
            update_obersvation(observation)
        return None
    else:
        return {
            "Datastream" :  {
                "@iot.id" : datastream['@iot.id']
            },
            "components" : [
                "phenomenonTime",
                "resultTime",
                "result"
            ],
            "dataArray" : [
                [
                    phenomenonTime,
                    datetime.datetime.now().astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                    result['counts']
                ]
            ]
        }


def startOfStep(time, step):
    time = time.replace(second = 0, microsecond = 0)
    if step == INTERVAL_15_MIN_STEP:
        return time.replace(minute=math.floor(time.minute/15)*15)
    if step == INTERVAL_1_HOUR_STEP:
        return time.replace(minute=0)
    if step == INTERVAL_1_DAY_STEP:
        return time.replace(hour=0, minute=0)
    if step == INTERVAL_1_WEEK_STEP:
        return (time - datetime.timedelta(days=time.weekday())).replace(hour=0, minute=0)
    if step == INTERVAL_1_MONTH_STEP:
        return time.replace(day = 1, hour=0, minute=0)
    if step == INTERVAL_1_YEAR_STEP:
        return time.replace(month = 1, day = 1, hour=0, minute=0)
    return None

def getEndTime(start, step):
    if step == INTERVAL_15_MIN_STEP:
        return start + INTERVAL_15_MIN_DURATION
    if step == INTERVAL_1_HOUR_STEP:
        return start + INTERVAL_1_HOUR_DURATION
    if step == INTERVAL_1_DAY_STEP:
        return start + INTERVAL_1_DAY_DURATION
    if step == INTERVAL_1_WEEK_STEP:
        return start + INTERVAL_1_WEEK_DURATION
    if step == INTERVAL_1_MONTH_STEP:
        if start.month < 12:
            return start.replace(month=start.month+1)
        else:
            return start.replace(year=start.year+1, month=1)
    if step == INTERVAL_1_YEAR_STEP:
        return start.replace(year=start.year+1)
    return None


def load_things():
    print(FROST_THINGS_WITH_DATASTREAMS)
    results = []
    try:
        r = requests.get(FROST_THINGS_WITH_DATASTREAMS)
    except:
        r = requests.get(FROST_THINGS_WITH_DATASTREAMS)
    if (r.status_code == 200):
        json_response = r.json()
        if 'value' in json_response:
            results += json_response['value']
        else:
            results.append(json_response)
        while '@iot.nextLink' in json_response:
            try:
                r = requests.get(json_response['@iot.nextLink'])
            except:
                r = requests.get(json_response['@iot.nextLink'])
            if r.status_code != 200:
                print(str(r.status_code)+": "+r.json()['message'])
                raise Exception("Could not load Data from Frost")
            json_response = r.json()
            results += json_response['value']
    else:
        print("Error "+str(response.status_code))
        print(response.text)
        raise Exception('Could not load Data from Frost!')
    return results

@sched.scheduled_job('cron', hour=5, minute=45)
@sched.scheduled_job('cron', hour=9, minute=30)
def run_import():
    init_things()
    import_observations()

init()

#run_import()

print("Starting Scheduler")
sched.start()
print("End")
