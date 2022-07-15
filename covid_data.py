####################################
### Library
####################################
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
# import pyautogui

from bs4 import BeautifulSoup
import requests
import json

import pandas as pd
from datetime import *
import time

from pathlib import Path
import glob
import os
import shutil
from zipfile import ZipFile





####################################
### Parameter
####################################
today = datetime.today()

### Web Scraping Link ###
## Raw Data 1 Link  -  List of buildings visited by cases tested positive Covid in the past 14 days
infect_build_web = "https://data.gov.hk/tc-data/dataset/hk-dh-chpsebcddr-novel-infectious-agent/resource/87be0ebc-bb93-46cf-ac90-bce0769cb72e"
## Raw Data 2 Link  -  Latest situation of reported cases of COVID-19 in Hong Kong
current_infect_web = "https://data.gov.hk/tc-data/dataset/hk-dh-chpsebcddr-novel-infectious-agent/resource/88d1ea88-658f-434d-a482-4c33be291fdf"
## Raw Data 3 Link  -  Overall Statistics on 5th Wave of COVID-19
overall_infect_web = "https://www.coronavirus.gov.hk/chi/5th-wave-statistics.html"


### Folder Directory ###
## Main folder
folder_path = r'C:\Users\Gavin_Lam\Desktop\New folder'                                             # main folder location
## Subfolder, create daily and store daily raw data
download_directory = os.path.join(folder_path, today.strftime('%Y%m%d'))





####################################
### Driver Initialization
####################################
driver_path = "chromedriver.exe"                                                                   # chromedriver location

options = Options() 
options.add_experimental_option('prefs', {'download.default_directory' : download_directory})      # downloaded files destination
# options.add_experimental_option('excludeSwitches', ['enable-logging'])
# options.add_argument("--headless")





def get_covid_data():


    def daily_download():

        ####################################
        ### Web Scraping
        ####################################

        ### Function ###
        ## Function - Explicit Wait 
        def locator(typ, nam, sec=10):
            return WebDriverWait(driver, sec).until(EC.presence_of_element_located((typ, nam)))

        ## Function - data download in data.gov.hk                                         
        # same download procedure for all dataset
        def data_gov_download():
            locator(By.XPATH, '//button[@class="p-secondary-button p-bold-button icon-print-queue-add btn"]').click()
            time.sleep(1)
            locator(By.CLASS_NAME, "modal-open").click()
            time.sleep(1)
            locator(By.XPATH, '//div[@class="not-display-mobile popup download-queue-btn"]').click()
            time.sleep(1)
            locator(By.XPATH, '//button[@class="p-secondary-button btn p-submit-queue"]').click()
            time.sleep(1)
            locator(By.XPATH, '//button[@class="p-primary-button p-bold-button p-download-btn btn"]', 30).click()


        ### Scraping Zip Files ###
        ## Raw Data 1 Zip
        driver = webdriver.Chrome(driver_path, options=options)
        driver.get(infect_build_web)
        data_gov_download()
        time.sleep(2)
        driver.quit()

        ## Raw Data 2 Zip
        driver = webdriver.Chrome(driver_path, options=options)
        driver.get(current_infect_web)
        data_gov_download()
        time.sleep(2)

        ## Raw Data 3 Zip
        driver.get(overall_infect_web)
        ol_date_pick = (today - pd.DateOffset(days=1)).strftime('%Y%m%d')
        ol_button = f'(//img[@title="Breakdown_by_age group_{ol_date_pick}.csv"])'
        locator(By.XPATH, ol_button).click()
        time.sleep(2)
        driver.quit()





        ####################################
        ### Extract Zip Files
        ####################################

        ## Unzip & delete zip files
        for file in Path(download_directory).glob('*.zip'):
            with ZipFile(file, 'r') as zip:
                zip.extractall(file.parents[0])
            os.remove(file)

        ## Move all files from subfolders to main folder
        for folder in Path(download_directory).glob('*'):
            try:
                for file in Path(str(folder)).glob('*'):
                    shutil.move(str(file), download_directory)
                # Delete subfolder
                shutil.rmtree(folder)
            except:
                # Non folder item pass
                pass


    ### If folder not exist, means today data not been downloaded yet
    if not os.path.exists(download_directory):
        ## Create Subfolder if not exist
        Path(download_directory).mkdir(parents=True, exist_ok=True)
        daily_download()





    ####################################
    ### Data Update  -  Merge & Clean
    ####################################

    ### [Data 1] - Building list with Geo data ###
    ## [Raw Data 1] - Building list
    df_building = pd.read_csv(os.path.join(download_directory, 'building_list_chi.csv')
                            # ,converters = {'個案最後到訪日期':pd.to_datetime}
                            )
    df_building['個案最後到訪日期'] = pd.to_datetime(df_building['個案最後到訪日期'], format= '%d/%m/%Y')

    ## [Raw Data 4] - Cumulative address list with Latitude and Longitude data
    df_ogcio = pd.read_csv(os.path.join(folder_path, 'ogcio.csv'))


    ## Series - listing new buildings with no geo data 
    df_new_building = pd.concat([df_building['大廈名單'], df_ogcio['大廈名單']]).drop_duplicates(keep=False)
    ## Empty dataframe
    df_temp_ogcio = pd.DataFrame(columns=['大廈名單','Latitude','Longitude'])


    ## Function - get HK Address Latitude and Longitude data from ogcio API
    def ogcio(Address):
        session = requests.Session()
        ogcio_url = "https://www.als.ogcio.gov.hk/lookup?"

        # https://data.gov.hk/tc-data/dataset/hk-ogcio-st_div_02-als/resource/82c99adf-56c9-4af4-bb48-9c22a631411d
        # Input parameters:
        # "q" – input address element information (mandatory; URL-encoded(percent-encoded));
        # "n" – maximum number of records to be returned (optional; range: 1-200; default: 200);
        # "t" – tolerance on returned record scores (optional; range: 0-80; default: 20);
        # "b" – enable/disable basic searching mode, default disabled (optional; range: 0 or 1; default: 0).
        r = session.get(ogcio_url, 
                        headers={"Accept": "application/json"},
                        params={"q": Address, 
                                "n": 10})
        soup = BeautifulSoup(r.content, 'html.parser')   

        if 'SuggestedAddress' in json.loads(str(soup)):
            return(json.loads(str(soup))['SuggestedAddress'])
        else:
            return None


    ## Download geo data for new building address;   would take some time~
    for i in range(len(df_new_building)):
        print( f'[{i} / {len(df_new_building)}]')
        Address = df_new_building.iloc[i]
        result = ogcio(Address)

        if len(result)>0:
            la = result[0]['Address']['PremisesAddress']['GeospatialInformation']['Latitude']
            long = result[0]['Address']['PremisesAddress']['GeospatialInformation']['Longitude']
            
            dict = {'大廈名單': Address, 'Latitude': la, 'Longitude': long}
            df_temp_ogcio = df_temp_ogcio.append(dict, ignore_index = True)


    ## Update [Raw Data 4]
    df_ogcio = pd.concat([df_ogcio, df_temp_ogcio], ignore_index=True)
    ## Save new [Raw Data 4]
    df_ogcio['update_time'] = datetime.now()
    df_ogcio.to_csv(os.path.join(folder_path, 'ogcio.csv'), encoding='utf_8_sig', index = False)

    ### [Data 1] - merging [Raw Data 1] with geo data
    df_building_with_geo = pd.merge(df_building, df_ogcio, how='left', on=['大廈名單'])
    df_building_with_geo['update_time'] = datetime.now()
    ## Save
    df_building_with_geo.to_csv(os.path.join(folder_path, 'building_list.csv'), encoding='utf_8_sig', index = False)





    ### [Data 2] - Latest COVID situation ###
    ## [Raw Data 2]
    df_latest = pd.read_csv(os.path.join(download_directory,'latest_situation_of_reported_cases_covid_19_chi.csv')
                            # ,converters = {'更新日期':pd.to_datetime}
                            )
    df_latest['更新日期'] = pd.to_datetime(df_latest['更新日期'], format= '%d/%m/%Y')       
    df_latest['update_time'] = datetime.now()
    ## Save
    df_latest.to_csv(os.path.join(folder_path, 'current_situation.csv'), encoding='utf_8_sig', index = False)





    ### [Data 3] - Overall COVID situation ###
    # use glob:  csv names are diff. by each date
    overall_path = glob.glob(os.path.join(download_directory, 'Breakdown*.csv'))[0]          
    ## [Raw Data 3]  &  Clean
    df_overall = pd.read_csv(overall_path, skiprows=3, nrows= 13)
    df_overall['update_time'] = datetime.now()
    ## Save
    df_overall.to_csv(os.path.join(folder_path, 'overall_situation.csv'), encoding='utf_8_sig', index = False)





if __name__ == '__main__':
    try:
        get_covid_data()
        print('Success !!!')
    except Exception as e:
        print('Fail~')
        print(str(e))

