import sys
import requests
import time
import re
import sqlite3
from collections import deque
from tqdm import tqdm

import csv
from io import StringIO, BytesIO
import zipfile
from datetime import datetime

from xml.etree import ElementTree as ET


def generalized_insert_records(
    conn: sqlite3.Connection,
    table_name: str,
    cols: list[str],
    records: list[dict],
    ignore: bool = True,
    commit: bool = False,
) -> None:
    """
    Generalized method to insert data into db. Can be customized closer to data structure.

    Args:
        conn (sqlite3.Connection): Active connection to the SQLite database.
        table_name (str): name of the table to insert into
        cols (list[str]): list of the names of the record keys, will be used for columns
        records (list[dict]): list of dict that contain cols as keys
        ignore (bool): whether to ignore duplicate unique entries
        commit (bool): whether to immediately commit the database

    Returns:
        None: Data is stored directly into the database.
    """
    ignore_or_replace = "IGNORE" if ignore else "REPLACE"

    cursor = conn.cursor()

    cols_str = ", ".join(cols)
    placeholders = ", ".join("?" * len(cols))
    query = f"""
        INSERT OR {ignore_or_replace}
        INTO {table_name}
        ({cols_str})
        VALUES
        ({placeholders})
    """

    values = [[record[col] for col in cols] for record in records]

    cursor.executemany(query, values)
    if commit:
        conn.commit()
    return


def generalized_update_records(
    conn: sqlite3.Connection,
    table_name: str,
    cols: list[str],
    keycols: list[str],
    records: list[dict],
    commit: bool = False,
) -> None:
    """
    Generalized method to update data into db. Can be customized closer to data structure.

    Args:
        conn (sqlite3.Connection): Active connection to the SQLite database.
        table_name (str): name of the table to insert into
        cols (list[str]): list of the names of the record keys to update
        keycols (list[str]): list of the names of the record keys to use for id
        records (list[dict]): list of dict that contain cols as keys
        commit (bool): whether to immediately commit the database

    Returns:
        None: Data is stored directly into the database.
    """
    cursor = conn.cursor()

    cols_placeholders = ", ".join([f"{col}=?" for col in cols])
    keys_placeholders = " AND ".join([f"{key}=?" for key in keycols])
    query = f"""
        UPDATE
        {table_name}
        SET
        {cols_placeholders}
        WHERE
        {keys_placeholders}
    """

    values = [
        [record[col] for col in cols] + [record[key] for key in keycols]
        for record in records
    ]

    cursor.executemany(query, values)
    if commit:
        conn.commit()
    return


def fetch_13f_hr_links(year, quarter):
    base_url = "https://www.sec.gov"
    index_url = f"{base_url}/Archives/edgar/full-index/{year}/QTR{quarter}/master.zip"
    headers = {
        "User-Agent": "ConjecturalCap Contact: ConjecturalCap@gmail.com",
        "Accept-Encoding": "gzip, deflate",
    }
    response = requests.get(index_url, headers=headers)
    response.raise_for_status()  # Raises an HTTPError for bad responses (4XX, 5XX)

    accessionnumber_pattern = re.compile(r"/([\d-]+)\.txt")

    links = []
    with zipfile.ZipFile(BytesIO(response.content)) as z:
        with z.open("master.idx") as idx_file:
            # Create a TextIOWrapper to read the file as text
            f = StringIO(idx_file.read().decode("utf-8"))
            reader = csv.reader(f, delimiter="|")

            # Process each row in the index file
            for row in reader:
                if row and len(row) == 5:
                    if "13F-HR" in row[2]:
                        cik = int(row[0].strip())
                        filing_date = int(row[3].strip().replace("-", ""))
                        match = accessionnumber_pattern.search(row[4].strip())
                        if match:
                            accession_number = int(match.group(1).replace("-", ""))
                            links.append(
                                {
                                    "cik": cik,
                                    "access_number": accession_number,
                                    "filing_date": filing_date,
                                }
                            )
    return links


def save_filings_to_db(conn, filing_list):
    generalized_insert_records(
        conn, "filings", ["cik", "access_number", "filing_date"], filing_list
    )

    # cursor = conn.cursor()
    # cursor.executemany(
    #     """
    #     INSERT OR IGNORE INTO
    #         filings
    #     (cik, access_number, filing_date)
    #         VALUES
    #     (?,?,?)
    # """,
    #     filing_list,
    # )
    # conn.commit()
    return


def incremental_run(conn, last_processed_date=None):
    today = datetime.now().date()
    if last_processed_date is None:
        last_processed_date = "20190101"
    if isinstance(last_processed_date, str):
        last_processed_date = datetime.strptime(last_processed_date, "%Y%m%d")

    last_processed_quarter = (last_processed_date.month - 1) // 3 + 1
    last_processed_year = last_processed_date.year

    current_year = today.year
    current_quarter = (today.month - 1) // 3 + 1

    years_to_process = range(last_processed_year, current_year + 1)
    for year in years_to_process:
        start_quarter = last_processed_quarter if year == last_processed_year else 1
        end_quarter = current_quarter if year == current_year else 4
        for quarter in range(start_quarter, end_quarter + 1):
            print(f"Fetching for Year: {year}, Quarter: {quarter}")
            links = fetch_13f_hr_links(year, quarter)
            # for link in links:
            # print(link)
            save_filings_to_db(conn, links)
            time.sleep(1)  # SEC's rate limit compliance

    # Update the last processed date at the end of the run
    return


def finish_holdings_insert(conn):
    cursor = conn.cursor()
    cursor.execute(
        """
CREATE TABLE IF NOT EXISTS
holdings
(
access_number INTEGER NOT NULL,
cusip_id INTEGER NOT NULL,
shares INTEGER NOT NULL,
value INTEGER NOT NULL,
PRIMARY KEY(access_number, cusip_id)
)
"""
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS
cusips
    (
        cusip_id INTEGER PRIMARY KEY AUTOINCREMENT,
        cusip TEXT NOT NULL,
        is_sh INTEGER NOT NULL,
        is_put INTEGER NOT NULL,
        is_call INTEGER NOT NULL,
        UNIQUE(cusip, is_sh, is_put, is_call)
    )

"""
    )
    cursor.execute(
        """
INSERT OR IGNORE INTO
cusips
(cusip, is_sh, is_put, is_call)
SELECT
cusip,
is_sh,
is_put,
is_call
FROM holdings_staging
"""
    )
    cursor.execute(
        """
INSERT OR REPLACE INTO
holdings
(access_number, cusip_id, shares, value)
SELECT
hs.access_number,
c.cusip_id,
hs.shares,
hs.value
FROM holdings_staging hs
LEFT JOIN cusips c
ON
c.cusip = hs.cusip
AND c.is_sh = hs.is_sh
AND c.is_put = hs.is_put
AND c.is_call = hs.is_call
"""
    )
    cursor.execute(
        """
DELETE
FROM holdings_staging
WHERE 1=1
"""
    )
    conn.commit()
    return


def insert_metadata(conn, meta_dict):
    # cursor = conn.cursor()
    cols = [
        "report_date",
        "amendment_number",
        "is_restated",
        "entry_value",
        "entry_count",
        "is_confidential",
        "is_discrepant",
    ]
    keycols = ["access_number"]

    generalized_update_records(conn, "filings", cols, keycols, [meta_dict])

    # placeholders = ",".join([f"{k}=?" for k in keys])
    # values = [meta_dict[k] for k in keys] + [meta_dict["access_number"]]
    # cursor.execute(
    #     f"""
    #     UPDATE
    #         filings
    #     SET
    #         {placeholders}
    #     WHERE
    #         access_number=?
    #     """,
    #     values,
    # )
    return


def insert_holdings(conn, holdings_list):
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS
        holdings_staging
        (
            access_number INTEGER,
            cusip TEXT NOT NULL,
            is_sh INTEGER,
            is_put INTEGER,
            is_call INTEGER,
            shares INTEGER,
            value INTEGER,
            UNIQUE(access_number, cusip, is_sh, is_put, is_call)
        )
    """
    )
    keys = ["access_number", "cusip", "is_sh", "is_put", "is_call", "shares", "value"]

    generalized_insert_records(
        conn, "holdings_staging", keys, holdings_list, ignore=False
    )

    # columns = ",".join(keys)
    # placeholders = ",".join("?" * len(keys))
    # values = [[x[k] for k in keys] for x in holdings_list]
    # cursor.executemany(
    #     f"""
    #         INSERT OR REPLACE INTO
    #         holdings_staging
    #         ({columns})
    #         VALUES
    #         ({placeholders})
    #     """,
    #     values,
    # )
    return


def parse_date(date_str):
    from dateutil import parser

    try:
        parsed_date = parser.parse(date_str, fuzzy=True)
        if (parsed_date.month, parsed_date.day) in (
            (3, 31),
            (6, 30),
            (9, 30),
            (12, 31),
        ):
            return parsed_date.strftime("%Y%m%d")
        else:
            raise ValueError(f"The date {parsed_date} is not a valid quarter-end date.")
    except Exception as e:
        raise ValueError(
            f"The date {parsed_date} is not a valid quarter-end date. Error: \n{e}"
        )
    return None


def convert_access_number(n):
    s = str(n).zfill(18)
    s = f"{s[:10]}-{s[10:12]}-{s[12:]}"
    return s


def strip_namespace_from_xml(xml):
    xml = re.sub(r'\s+xmlns(:\w+)?="[^"]+"', "", xml, flags=re.DOTALL)
    xml = re.sub(r"\s+xmlns(:\w+)?='[^'']+'", "", xml, flags=re.DOTALL)

    xml = re.sub(r'\s+xsi(:\w+)?="[^"]+"', "", xml, flags=re.DOTALL)
    xml = re.sub(r"\s+xsi(:\w+)?='[^']+'", "", xml, flags=re.DOTALL)

    xml = re.sub(r"<(/?)(\w+):", r"<\1", xml)
    return xml


def parse_sec_13f_hr(cik, access_number):
    """
    Parses an SEC 13F-HR filing given its URL, extracting metadata and holdings.

    Parameters:
        url (str): URL to the SEC 13F-HR XML file.

    Returns:
        meta_df (pd.DataFrame): DataFrame with metadata of the filing.
        holdings_df (pd.DataFrame): DataFrame with holdings information.
    """
    # Download the XML content

    access_number_str = convert_access_number(access_number)
    url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{access_number_str}.txt"
    headers = {"user-agent": "SEC Agent secagent@sec.gov"}

    response = requests.get(url, headers=headers)
    response.raise_for_status()

    full_text = response.text
    full_text = strip_namespace_from_xml(full_text)
    xml_docs = re.findall(r"<XML>\s*(.*?)\s*</XML>", full_text, re.DOTALL)

    # Extract metadata
    root = ET.fromstring(xml_docs[0])
    metadata = {}

    def extract_data(root, path, convert_func):
        txt = root.findtext(path)
        return convert_func(txt) if txt else 0

    def is_in_str(test: str, string: str) -> int:
        return 1 if test.upper() in string.upper() else 0

    metadata["access_number"] = access_number
    metadata["entry_count"] = extract_data(
        root, "formData/summaryPage/tableEntryTotal", int
    )
    # metadata["entry_count"] = root.findtext("formData/summaryPage/tableEntryTotal")
    # metadata["entry_count"] = (
    #     int(metadata["entry_count"]) if metadata["entry_count"] else 0
    # )

    metadata["entry_value"] = extract_data(
        root, "formData/summaryPage/tableValueTotal", int
    )
    # metadata["entry_value"] = root.findtext("formData/summaryPage/tableValueTotal")
    # metadata["entry_value"] = (
    #     int(metadata["entry_value"]) if metadata["entry_value"] else 0
    # )

    metadata["is_confidential"] = extract_data(
        root,
        "formData/summaryPage/isConfidentialOmitted",
        lambda x: is_in_str("TRUE", x),
    )
    # metadata["is_confidential"] = root.findtext("formData/summaryPage/isConfidentialOmitted")
    # metadata["is_confidential"] = (
    #     "true" in metadata["is_confidential"].lower()
    #     if metadata["is_confidential"]
    #     else False
    # )

    metadata["amendment_number"] = extract_data(
        root, "formData/coverPage/amendmentNo", int
    )
    # metadata["amendment_number"] = root.findtext("formData/coverPage/amendmentNo")
    # metadata["amendment_number"] = (
    #     int(metadata["amendment_number"]) if metadata["amendment_number"] else 0
    # )

    metadata["is_restated"] = extract_data(
        root,
        "formData/coverPage/amendmentInfo/amendmentType",
        lambda x: is_in_str("RESTAT", x),
    )
    # metadata["is_restated"] = root.findtext("formData/coverPage/amendmentInfo/amendmentType")
    # metadata["is_restated"] = (
    #     "restat" in metadata["is_restated"].lower()
    #     if metadata["is_restated"]
    #     else False
    # )

    metadata["report_date"] = extract_data(
        root, "formData/coverPage/reportCalendarOrQuarter", parse_date
    )
    # metadata["report_date"] = root.findtext("formData/coverPage/reportCalendarOrQuarter")
    # metadata["report_date"] = parse_date(metadata["report_date"])

    metadata["is_discrepant"] = 0

    root = ET.fromstring(xml_docs[1])

    holdings_dict = {}
    table_entries = 0
    table_value = 0
    for info_table in root.findall(".//infoTable"):

        table_entries += 1
        table_value += extract_data(info_table, "value", int)

        discretion = info_table.findtext("investmentDiscretion")
        if discretion and "sole" in discretion.lower():

            cusip = extract_data(info_table, "cusip", str.upper)

            is_sh = extract_data(
                info_table,
                "shrsOrPrnAmt/sshPrnamtType",
                lambda x: is_in_str("SH", x),
            )

            is_put = extract_data(
                info_table,
                "putCall",
                lambda x: is_in_str("PUT", x),
            )

            is_call = extract_data(
                info_table,
                "putCall",
                lambda x: is_in_str("CALL", x),
            )

            shares = extract_data(info_table, "shrsOrPrnAmt/sshPrnamt", int)
            value = extract_data(info_table, "value", int)

            cusip_tuple = (cusip, is_sh, is_put, is_call)
            if cusip_tuple not in holdings_dict:
                holdings_dict[cusip_tuple] = {
                    "access_number": access_number,
                    "cusip": cusip,
                    "is_sh": is_sh,
                    "is_put": is_put,
                    "is_call": is_call,
                    "shares": shares,
                    "value": value,
                }
            else:
                holdings_dict[cusip_tuple]["shares"] += shares
                holdings_dict[cusip_tuple]["value"] += value

    if (
        table_entries != metadata["entry_count"]
        or abs(table_value - metadata["entry_value"]) > 2 * table_entries
    ):
        metadata["is_discrepant"] = 1

    return metadata, holdings_dict.values()


if __name__ == "__main__":
    conn = sqlite3.connect("holdings_analysis.sqlite")
    cursor = conn.cursor()

    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS
      filings
    (
        cik INTEGER,
        access_number INTEGER,
        filing_date INTEGER,
        report_date INTEGER,
        amendment_number INTEGER,
        is_restated INTEGER,
        entry_value INTEGER,
        entry_count INTEGER,
        is_confidential INTEGER,
        is_discrepant INTEGER,
        PRIMARY KEY (cik, access_number)
    )
    """
    )

    cursor.execute(
        """
        SELECT MAX(filing_date) as max_date
        FROM filings
    """
    )
    last_processed_date = str(cursor.fetchone()[0])

    print(f"Getting list of 13F filings since {last_processed_date}")
    incremental_run(conn, last_processed_date=last_processed_date)

    query = """
        SELECT cik, access_number
        FROM filings
        WHERE report_date IS NULL
        ORDER BY filing_date DESC
        --ORDER BY RANDOM()
    """
    filing_list = cursor.execute(query).fetchall()
    filing_list_limit = len(filing_list)
    if len(sys.argv) > 1:
        try:
            filing_list_limit = int(sys.argv[1])
        except Exception as e:
            raise ValueError(
                f"1st cl argument needs to be a positive integer. Error: {e}"
            )

    print(f"Downloading {filing_list_limit} of {len(filing_list)} filings")

    rate_limit_deque = deque()
    for _ in range(10):
        rate_limit_deque.append(time.time())

    cache_counter = 0
    cache_limit = filing_list_limit + 1
    if len(sys.argv) > 2:
        try:
            cache_limit = int(sys.argv[2])
        except Exception as e:
            raise ValueError(
                f"2nd cl argument needs to be a positive integer. Error: {e}"
            )

    for cik, access_number in tqdm(filing_list[:filing_list_limit]):
        metadata, holdings = parse_sec_13f_hr(cik, access_number)

        if holdings:
            insert_holdings(conn, holdings)
            insert_metadata(conn, metadata)

        rate_limit_check = max(rate_limit_deque.popleft() + 1.0 - time.time(), 0.0)
        time.sleep(rate_limit_check)
        rate_limit_deque.append(time.time())

        cache_counter += 1
        if cache_counter > cache_limit:
            finish_holdings_insert(conn)
            cache_counter = 0

    finish_holdings_insert(conn)

    conn.close()

    print("COMPLETE")
