"""
goinfra.py: Script for scraping and get screenshots of products
"""
import os
import time
import locale
import logging
import pandas as pd
from pathlib import Path
from selenium import webdriver
from unidecode import unidecode
from datetime import datetime, timedelta
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from airflow.operators.python import PythonOperator
from selenium.webdriver.common.action_chains import ActionChains
from constants import clean_print

logging = logging.getLogger("airflow.task")

HOST = "host.docker.internal"
DEFAULT_PATH = "./data_output"
INPUT_DIR = "./data_input"


def get_date(config: dict) -> datetime:
    """
    Return datetime minus difference setting on yml
    Return:
        date [datetime]
    """
    return datetime.today() - timedelta(config["day_diff"])


def get_path(date: datetime, output_dir: str) -> str:
    """
    Return export path folder based on uppercase name **see dag.py**
    Return:
        path [str]
    """
    locale.setlocale(locale.LC_ALL, "pt_BR.UTF-8")
    path = date.strftime(
        f'{DEFAULT_PATH}/{output_dir}/output_%Y_%m_%d'
    )
    Path(f"{path}/prints").mkdir(
        parents=True, exist_ok=True
    )  # Output prints folder creation

    return path


def cleaner(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Quick cleanup of the column called in the class.
    All dataframes must contain a product name column.

    Args:
        dataframe readed
    Return:
        dataframe with cleaner_name columnn and url cleaned
    """
    logging.info(f"Cleaning the column {'name'}")
    dataframe["cleaner_name"] = dataframe["name"].copy()
    dataframe["url"] = dataframe["url"].str.lower()

    for cleaner_elements in clean_print:
        dataframe["cleaner_name"] = dataframe["cleaner_name"].str.replace(
            cleaner_elements, "", regex=False
        )
    dataframe["cleaner_name"] = dataframe["cleaner_name"].apply(unidecode)

    return dataframe


def prep(config: dict, output_dir: str):
    """
    Read requested itens and filtered by INFORM and PERIOD

    Args: dataframe [pd.DataFrame], config [dict]
    Return: filtered dataframe
    """
    logging.info(
        f"Reading the last request file for day {get_date(config).strftime('%d/%m/%Y')}"
    )

    data = []
    for files in os.listdir(INPUT_DIR):
        if files.startswith("data") and files.endswith("csv"):
            data.append(files)

    dataframe = pd.read_csv(
        f"{INPUT_DIR}/{data[-1]}",
        delimiter=";",
        dtype=str,
    )
    dataframe = cleaner(dataframe)

    logging.info(f"Filtering for Codes {config['code']}")
    result_prep = dataframe[
        dataframe.code.isin([config["code"]])
    ]
    export = get_path(date=get_date(config), output_dir=output_dir)
    result_prep.to_csv(f"{export}/prep.csv", index=False)

    logging.info(f"Saved prep dataframe in {export}")


def config_browser():
    """
    Browser configuration to remove screenshots
    It is important to note that the recommendation is not to use headless.

    Return: browser [class object]
    """
    capabilities = {
        "browserName": "firefox",
        "browserVersion": "105.0",
        "selenoid:options": {
            "enableVideo": False
        }
    }
    logging.info("Instantiating the Browser with the requested settings")
    browser = webdriver.Remote(
        command_executor="http://{}:4444/wd/hub".format(HOST),
        desired_capabilities=capabilities)
    logging.info(f"Browser configured")
    # browser = webdriver.Firefox(
    #     service=FirefoxService(GeckoDriverManager().install()),
    #     options=options,
    # )

    return browser


def verify_integrity(dataframe: pd.DataFrame, price: list):
    """
    Integrity check between collected prices
    and requested items
    """
    if len(price) == len(dataframe["url"]):
        logging.info("Test passed")

    else:
        logging.info("Test not passed")
        logging.info(
            f"Difference of {len(dataframe['url']) - len(price)} prices"
        )


def append_data(
    date: datetime, dataframe: pd.DataFrame, price: list, url: list, path: str, output_dir: str
):
    """
    Merge of collected data and requested items
    """
    verify_integrity(dataframe=dataframe, price=price)
    results = pd.DataFrame(
        {"price": price, "url_collected": url, "shipping": ""}
    )

    results["date"] = date.strftime("%d/%m/%Y")

    logging.info(f"Requested prices: {len(dataframe['url'])}")
    logging.info(f"Scraped prices: {len(price)}")
    logging.info(f"Day of the scraping: {date.strftime('%d/%m/%Y')}")

    result_colector = dataframe.merge(
        results, left_on="url", right_on="url_collected", how="left"
    )
    result_colector.to_csv(f"{path}/colector.csv", index=False)
    export = date.strftime(f"{path}/{output_dir}_%d_%m_%Y.xls")
    result_colector.to_excel(f"{export}", index=False)


def nan_remove(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Args:
        dataframe:
    Returns: Filtered URL Column without values
    """
    logging.info("Removing nan URL values")
    logging.info(f"Requested prices before: {len(dataframe['url'])}")

    dataframe_new = dataframe.dropna(subset=["url"])
    dataframe_new = dataframe_new[dataframe_new["url"] != ""]

    logging.info(f"Requested prices after: {len(dataframe_new['url'])}")

    return dataframe_new


def scroll(config: dict, browser):
    """
    If scroll is enable in config, they will insert
    down key to updown page multiplying based on value.
    """
    actions = ActionChains(browser)
    if "scroll" in config:
        logging.info(f"Scrolling pages on {config['scroll']['value']} keys")
        actions.send_keys(eval(config["scroll"]["type"]) * config["scroll"]["value"])
        actions.perform()
        time.sleep(config["delay"])


def printer(
    config: dict, browser, code: str, name: str, date: datetime, path: str
):
    """
    Apply print and scroll page if needed
    """
    logging.info(f"Printing information")
    scroll(config=config, browser=browser)
    browser.save_screenshot(
        f"{path}/prints/{code}_{name}_{date.strftime('%d-%m-%Y')}.png"
    )


def apply_cookies(config, browser, dataframe: pd.DataFrame):
    """
    Apply cookies to browser if needed
    """
    if "cookies" in config:
        browser.get(dataframe["url"].to_list()[0])
        logging.info(f"Applying cookies to browser")
        for name, value in config["cookies"].items():
            browser.add_cookie({"name": name, "value": value})
        browser.refresh()


def colector(config, output_dir):
    """
    Using an iteration on urls, names and dataframe elements.
    We proceed, in order, to extract a 'screenshot' with the element and the name of the product.
    """
    price = []
    url = []

    date = get_date(config=config)
    path = get_path(date=date, output_dir=output_dir)

    logging.info("Reading prep dataframe")
    dataframe = pd.read_csv(f"{path}/prep.csv", dtype=str)
    browser = config_browser()
    dataframe_new = nan_remove(dataframe=dataframe)
    apply_cookies(config=config, browser=browser, dataframe=dataframe_new)

    for link, code, name in zip(
        dataframe_new["url"].to_list(),
        dataframe_new["code"].to_list(),
        dataframe_new["cleaner_name"].to_list(),
    ):

        try:
            browser.get(link)
            time.sleep(config["delay"])
            value = browser.find_element(
                eval(config["type_element"]), config["element"]
            ).text
            price.append(value)
            url.append(link)
            logging.info(f"Price of {code} - {name}: {value}")

        except Exception as e:
            logging.info(f"Skipping element errors to: {link}")
            logging.info(f"Error: {e}")
            pass

        printer(
            config=config,
            browser=browser,
            code=code,
            name=name,
            date=date,
            path=path,
        )

    logging.info(f"Saved screenshots and prices")
    return append_data(
        dataframe=dataframe, price=price, url=url, date=date, path=path, output_dir=output_dir
    )


def create_task_prep(config, output_dir):
    prep_task = PythonOperator(
        task_id="prep",
        provide_context=True,
        python_callable=globals()["prep"],
        op_kwargs={"config": config, "output_dir": output_dir},
    )

    return prep_task


def create_task_collector(config, output_dir):
    collector_task = PythonOperator(
        task_id="colector",
        provide_context=True,
        python_callable=globals()["colector"],
        op_kwargs={"config": config, "output_dir": output_dir},
    )

    return collector_task
