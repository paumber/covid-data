#############
# STILL TO DO
# Upload Lancs file to drive
# Is it necessary to save csv file locally before uploading? Can we skip a step?
#############


import pandas as pd
from datetime import datetime
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from datetime import datetime

# URLs for raw weekly case data at MSOA level published by PHE
urlBpl = "https://api.coronavirus.data.gov.uk/v2/data?areaType=msoa&areaCode=E06000009&metric=newCasesBySpecimenDateRollingRate&format=csv"
urlPre = "https://api.coronavirus.data.gov.uk/v2/data?areaType=msoa&areaCode=E07000123&metric=newCasesBySpecimenDateRollingRate&format=csv"
urlWig = "https://api.coronavirus.data.gov.uk/v2/data?areaType=msoa&areaCode=E08000010&metric=newCasesBySpecimenDateRollingRate&format=csv"
urlNorthWest = "https://api.coronavirus.data.gov.uk/v2/data?areaType=msoa&areaCode=E12000002&metric=newCasesBySpecimenDateRollingRate&format=csv"

#download PHE data and create dataframes to store them
csvBpl = pd.read_csv(urlBpl)
csvPre = pd.read_csv(urlPre)
csvWig = pd.read_csv(urlWig)
csvNorthWest = pd.read_csv(urlNorthWest)

# function to format data as we want it
def pivot(dFrame, dfIndex, dfCols):
    dfFiltered = dFrame[['areaCode', 'areaName', 'LtlaName', 'date', 'newCasesBySpecimenDateRollingRate']]
    newPivot = pd.pivot_table(dfFiltered, values='newCasesBySpecimenDateRollingRate', index=dfIndex, columns=dfCols)
    newPivot.fillna(0, inplace=True)
    return newPivot

def getGeo(dFrame):
    #dFrame.reset_index(inplace=True) ##still needed?
    #get geo info from csv
    geo = pd.read_csv('geo_lookup.csv')
    geo.name = 'geometry'
    ltlaList = ['Blackburn with Darwen', 'Blackpool', 'Burnley', 'Chorley', 'Fylde', 'Hyndburn', 'Lancaster', 'Pendle', 'Preston', 'Ribble Valley', 'Rossendale', 'South Ribble', 'West Lancashire', 'Wyre']

    # filter dFrame down to just Lancs and merge with geo
    dFrame = dFrame.reset_index()
    dfFinal = dFrame[dFrame['LtlaName'].isin(ltlaList)]
    dfFinal = dFrame.merge(geo, on='areaCode')

    #reorder columns to put geometry at front
    cols = list(dfFinal.columns)
    cols = cols[-1:] + (cols[:-1])
    dfFinal = dfFinal[cols]

    #don't think we need to save locally
    #dfFinal.to_csv('test_output.csv')
    return dfFinal

csvBpl = pivot(csvBpl, ['date'], ['areaName'])
csvPre = pivot(csvPre, ['date'], ['areaName'])
csvWig = pivot(csvWig, ['date'], ['areaName'])
csvNorthWest = pivot(csvNorthWest, ['areaCode', 'LtlaName', 'areaName'], ['date'])
csvNorthWest = getGeo(csvNorthWest)

# save data locally as csv file
# NOW OBSOLETE ??
# csvBpl.to_csv('Weekly_data_Bpl.csv')
# csvPre.to_csv('Weekly_data_Pre.csv')
# csvWig.to_csv('Weekly_data_Wig.csv')
#print('Three files (Bpl, Pre, Wig) updated locally at ' + datetime.now().strftime("%d/%m/%y %H:%M:%S"))
##print(csvNorthWest.columns)

# set up Google Drive access to upload file to publicly available location
gauth = GoogleAuth()
gauth.LocalWebserverAuth()
drive = GoogleDrive(gauth)

fileList = drive.ListFile({'q':"'root' in parents and trashed=false"}).GetList()
for file in fileList:
    # We want to upload file to foler 'python test' -  get its ID
    if(file['title'] == "Covid data"):
        fileID = file['id']

#upload files to Google Drive
fileBpl = drive.CreateFile({"mimeType": "text/csv", "title": "Weekly_data_Bpl.csv", "id": "1VxthucbvWJE44uLC6gLjieyIZi4pzco7", "parents": [{"kind": "drive#fileLink", "id": fileID}]})
fileBpl.SetContentString(csvBpl.to_csv(index=False))
fileBpl.Upload()
filePre = drive.CreateFile({"mimeType": "text/csv", "title": "Weekly_data_Pre.csv", "id": "1dRiz7fyOJjietMquLUTXaBuPZ5YrdNWz", "parents": [{"kind": "drive#fileLink", "id": fileID}]})
filePre.SetContentString(csvPre.to_csv(index=False))
filePre.Upload()
fileWig = drive.CreateFile({"mimeType": "text/csv", "title": "Weekly_data_Wig.csv", "id": "1B00PcTt1jwlXU2_QqltpZm9XqLb5sgeL", "parents": [{"kind": "drive#fileLink", "id": fileID}]})
fileWig.SetContentString(csvWig.to_csv(index=False))
fileWig.Upload()
fileLancs = drive.CreateFile({"mimeType": "text/csv", "title": "Weekly_data_Lancs.csv", "id": "1kQwadGGNq0T6hwAN7XKB94KqQpkWOdyL", "parents": [{"kind": "drive#fileLink", "id": fileID}]})
fileLancs.SetContentString(csvNorthWest.to_csv(index=False))
fileLancs.Upload()
print('Uploaded four files (%s, %s, %s ,%s) to Google Drive at %s' % (fileBpl['title'], filePre['title'], fileWig['title'], fileLancs['title'], datetime.now().strftime("%d/%m/%y %H:%M:%S")))
