import requests
import sqlite3
from pqdm.threads import pqdm

met_url = "https://collectionapi.metmuseum.org/public/collection/v1/objects"

resp = requests.get(met_url)

resp_json = resp.json()

objects = resp_json["objectIDs"]

# create a database
conn = sqlite3.connect('met.db')
c = conn.cursor()

# create a table with fields:
# objectID, isHighlight, accessionNumber, accessionYear, isPublicDomain, primaryImage, primaryImageSmall, additionalImages, constituents, department, objectName, title, culture, period, dynasty, reign, portfolio, artistRole, artistPrefix, artistDisplayName, artistDisplayBio, artistSuffix, artistAlphaSort, artistNationality, artistBeginDate, artistEndDate, artistGender, artistWikidata_URL, artistULAN_URL, objectDate, objectBeginDate, objectEndDate, medium, dimensions, dimensionsParsed, measurements, creditLine, geographyType, city, state, county, country, region, subregion, locale, locus, excavation, river, classification, rightsAndReproduction, linkResource, metadataDate, repository, objectURL, tags, objectWikidata_URL, isTimelineWork, GalleryNumber

# check if table exists
c.execute('''SELECT count(name) FROM sqlite_master WHERE type='table' AND name='met' ''')
if c.fetchone()[0]==1 : 
    print('Table exists.')
# if table doesn't exist, create it
else:
    c.execute('''CREATE TABLE met (objectID text, isHighlight text, accessionNumber text, accessionYear text, isPublicDomain text, primaryImage text, primaryImageSmall text, additionalImages text, constituents text, department text, objectName text, title text, culture text, period text, dynasty text, reign text, portfolio text, artistRole text, artistPrefix text, artistDisplayName text, artistDisplayBio text, artistSuffix text, artistAlphaSort text, artistNationality text, artistBeginDate text, artistEndDate text, artistGender text, artistWikidata_URL text, artistULAN_URL text, objectDate text, objectBeginDate text, objectEndDate text, medium text, dimensions text, dimensionsParsed text, measurements text, creditLine text, geographyType text, city text, state text, county text, country text, region text, subregion text, locale text, locus text, excavation text, river text, classification text, rightsAndReproduction text, linkResource text, metadataDate text, repository text, objectURL text, tags text, objectWikidata_URL text, isTimelineWork text, GalleryNumber text)''')

# commit changes and close connection
conn.commit()
conn.close()

# create a function to get the data for each object
def get_data(objectID):
    url = f"{met_url}/{objectID}"
    resp = requests.get(url)
    resp_json = resp.json()
    # add object to database if object is a painting
    if resp_json["objectName"] == "Painting":
        conn = sqlite3.connect('met.db')
        c = conn.cursor()
        c.execute("INSERT INTO met VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,\
        ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", \
        (resp_json["objectID"], resp_json["isHighlight"], resp_json["accessionNumber"], resp_json["accessionYear"], resp_json["isPublicDomain"], resp_json["primaryImage"], resp_json["primaryImageSmall"], resp_json["additionalImages"], resp_json["constituents"], resp_json["department"], resp_json["objectName"], resp_json["title"], resp_json["culture"], resp_json["period"], resp_json["dynasty"], resp_json["reign"], resp_json["portfolio"], resp_json["artistRole"], resp_json["artistPrefix"], resp_json["artistDisplayName"], resp_json["artistDisplayBio"], resp_json["artistSuffix"], resp_json["artistAlphaSort"], resp_json["artistNationality"], resp_json["artistBeginDate"], resp_json["artistEndDate"], resp_json["artistGender"], resp_json["artistWikidata_URL"], resp_json["artistULAN_URL"], resp_json["objectDate"], resp_json["objectBeginDate"], resp_json["objectEndDate"], resp_json["medium"], resp_json["dimensions"], resp_json["dimensionsParsed"], resp_json["measurements"], resp_json["creditLine"], resp_json["geographyType"], resp_json["city"], resp_json["state"], resp_json["county"], resp_json["country"], resp_json["region"], resp_json["subregion"], resp_json["locale"], resp_json["locus"], resp_json["excavation"], resp_json["river"], resp_json["classification"], resp_json["rightsAndReproduction"], resp_json["linkResource"], resp_json["metadataDate"], resp_json["repository"], resp_json["objectURL"], resp_json["tags"], resp_json["objectWikidata_URL"], resp_json["isTimelineWork"], resp_json["GalleryNumber"]))
        conn.commit()
        conn.close()

        return True
    else:
        return False
    
# pqdm on 10 threads
pqdm(objects[112942:], get_data, n_jobs=50)
