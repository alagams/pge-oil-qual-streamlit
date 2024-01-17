import threading
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
import numpy as np
from queue import Queue
from queue import Empty
import concurrent.futures
import subprocess

T1_COLUMNS = ['Date', 'Btu Content', 'Secific Gravity', 'N2 Mole%', 'C02 Mole%', 
              'Methane Mole%', 'Ethane Mole%']
T2_COLUMNS = ['Date', 'Btu Content', 'Secific Gravity', 'N2 Mole%', 'C02 Mole%']
T3_COLUMNS = ['Date', 'Btu Content', 'Secific Gravity', 'N2 Mole%', 'C02 Mole%', 
              'Methane Mole%', 'Ethane Mole%', 'Propane Mole%', 'I-Butane Mole%', 
              'N-Butane Mole%', 'I-Pentane Mole%', 'N-Pentane Mole%', 'C6+ Mole%']

BTU_AREA_LIST = [
    "B01", "B02", "B03", "B04", "B05", "B06", "B08", "B09", "B10", "B11", "B12",
    "F01", "F02", "F03", "F04", "F05", "F06", "F07", "F08", "F09", "F10", "F11", "F12", "F13", "F14", "F15",
    "H01", "H03", "H04", "H07", "H09",
    "J01", "J02", "J03", "J05", "J07", "J08", "J10", "J12", "J13", "J15", "J16", "J17", "J18", "J19", "J20", "J21", "J22", "J24", "J28", "J29",
    "L01", "L02", "L03",
    "N01",
    "P01", "P02", "P04", "P05", "P06", "P08", "P11", "P14", "P15", "P16", "P18", "P19", "P21", "P22", "P24", "P59", "P60", "P78", "P81",
    "R01", "R02",
    "T01", "T02", "T09", "T10", "T14", "T28", "T29", "T80", "T81", "T82", "T83", "T84", "T85", "T86", "T87", "T88",
    "V01", "V02", "V03", "V05", "V06", "V07",
    "X01", "X02", "X04", "X05", "X07", "X09", "X10", "X12", "X15", "X17", "X18"
]

def getHtml(url, queue):
    # Set up Chrome options
    chrome_options = Options()
    # chrome_options.add_argument("--headless") 

    # chrome_driver_path = ChromeDriverManager().install()
    # print("ChromeDriver path:", chrome_driver_path)

    # Set up the Selenium driver
    driver = webdriver.Chrome()

    # Fetch the webpage
    print(url)
    driver.get(url)

    driver.implicitly_wait(20)

    WebDriverWait(driver, 10).until(
        EC.presence_of_all_elements_located((By.XPATH, '//table[@id="gasQualityTable"]/tbody/tr'))
    )


    # # Wait for the table element to be present on the page
    # WebDriverWait(driver, 100).until(
    #     EC.presence_of_element_located((By.ID, 'gasQualityTable'))
    # )

    driver.implicitly_wait(20)

    # Get the HTML of the page
    html = driver.page_source
    driver.quit()

    queue.put((url, html))

def parseHtml(pageQueue, parsedQueue):

    while True:
        try:
            url, html = pageQueue.get(timeout = 10)
        except Empty:
            return
        if html is None:
            break
        
        # Parse the HTML with BeautifulSoup
        soup = BeautifulSoup(html, 'html.parser')

        # Find the table by its ID
        table = soup.find('table', {'id': 'gasQualityTable'})
        rowsArray = []
        if table:
            rows = table.find_all('tr')
            for row in rows:
                cols = row.find_all('td')
                cols = [ele.text.strip() for ele in cols]
                rowsArray.append(cols)

            rowsArray = rowsArray[1:]

            try:
                # print(rowsArray)
                df = pd.DataFrame(np.array(rowsArray), columns = T1_COLUMNS)
                needCols = list(set(T3_COLUMNS)-set(T1_COLUMNS))
                for col in needCols:
                    df[col] = np.nan
            except ValueError as e:
                try:
                    df = pd.DataFrame(np.array(rowsArray), columns = T2_COLUMNS)
                    needCols = list(set(T2_COLUMNS)-set(T1_COLUMNS))
                    for col in needCols:
                        df[col] = np.nan
                except ValueError as e:
                    df = pd.DataFrame(np.array(rowsArray), columns = T3_COLUMNS)

            df = df.iloc[1:, :]
            df['Btu Area'] = url[-3:]
            # print(df.head(1))
            # print("\n")
            parsedQueue.put((url, df))
            # pageQueue.task_done()
        else:
            print("Table not found in the HTML.")

# def plotColByDay(df):
#     bxplt=df.boxplot(by='Date')
#     st.write(bxplt)

if __name__ == "__main__":
    # URL of the webpage containing the table
    btuAreaList = ['B01', 'B02', 'B03', 'B04', 'B05']
    urlList = [f'https://www.pge.com/pipeline/en/operating-data/therm/gas-quality-detail.html?btuId={area}' for area in BTU_AREA_LIST]
    pageQueue = Queue()
    parsedQueue = Queue()

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as getExe:
        results = list(map(lambda url: getExe.submit(getHtml, url, pageQueue), urlList))
        for future in concurrent.futures.as_completed(results):
            try:
                future.result()
            except Exception as e:
                print("Error is :", e, type(e))

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as parseExe:
        resultsParsed = list(map(lambda _: parseExe.submit(parseHtml, pageQueue, parsedQueue), range(10)))
        for future in concurrent.futures.as_completed(resultsParsed):
            try:
                future.result()
            except Exception as e:
                pass

    df_conc = pd.DataFrame
    firstFlag = True
    print(parsedQueue.qsize())
    while not parsedQueue.empty():
        try:
            _, df = parsedQueue.get()
            # print(df)
        except Empty:
            pass

        if firstFlag:
            firstFlag = False
            df_conc = df
        else:
            df_conc = pd.concat([df_conc, df], axis=0)
    # print(df_conc)
    df_conc.to_csv("out.csv")

    subprocess.Popen("streamlit run streamlitApp.py out.csv", shell=True)
