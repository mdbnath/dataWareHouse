import os, traceback
from re import sub
from typing import List, Dict, Optional
import pdfquery
import pandas as pd
from lxml import etree
import datetime
from dataWarehouseProject import dbInstance
con = dbInstance.getConnection(engine = True)

def write_xml(pdf_file_path: str, target_file_path: Optional[str] = None) -> 'etree.ElementTree':
    __target_file_path = target_file_path or pdf_file_path.replace('.pdf', '.xml')
    pdf = pdfquery.PDFQuery(pdf_file_path, normalize_spaces = True)
    pdf.load()
    pdf.tree.write(__target_file_path, pretty_print=True, encoding="utf-8")

def get_global_info(pdf: pdfquery.PDFQuery) -> pd.DataFrame:
    result_eudract: etree.ElementTree = pdf.pq(':contains("EudraCT number")')
    result_trial_protocol: etree.ElementTree = pdf.pq(':contains("Trial protocol")')
    result_trial_end_date: etree.ElementTree = pdf.pq(':contains("Global end of trial date")')
    publication_date: etree.ElementTree = pdf.pq(':contains("This version publication date")')
    
    column_idx = result_eudract[2].attrib['x0']
    eudract_idx = result_eudract[2].attrib['y0']
    result_trial_protocol_idx = result_trial_protocol[2].attrib['y0']
    result_trial_end_date_idx = result_trial_end_date[2].attrib['y0']
    publication_date_idx = publication_date[2].attrib['y0']
    found_values = False    
    page = result_eudract[1]
    temp = result_eudract[2]
    result = dict()
    while not found_values:
        temp = temp.getnext()

        # If end of page is reached, go to next page and continue
        if temp is None:
            page = page.getnext()
            if page is None:
                raise Exception('Reached end of file without finding all parameters!')
            temp = page[0]

        if temp.attrib['y0'] == eudract_idx and temp.attrib['x0'] != column_idx:
            result['eudract_number'] = temp.xpath("string()")
        elif temp.attrib['y0'] == result_trial_protocol_idx and temp.attrib['x0'] != column_idx:
            result['trial_protocol'] = temp.xpath("string()")
            print(type(temp.xpath("string()")))
        elif temp.attrib['y0'] == result_trial_end_date_idx and temp.attrib['x0'] != column_idx:
            print(type(str(temp.xpath("string()"))))
            print(str(temp.xpath("string()")))
            result['trial_end_date'] = datetime.datetime.strptime(str(temp.xpath("string()")).strip(),'%d %B %Y')

        elif temp.attrib['y0'] == publication_date_idx and temp.attrib['x0'] != column_idx:
            result['publication_date'] = datetime.datetime.strptime(str(temp.xpath("string()")).strip(),'%d %B %Y')
          
        
        if len(result) == 4:
            df = pd.DataFrame([result])
            
            print(df.dtypes)
            return df

def get_icu_rate(pdf: pdfquery.PDFQuery) -> pd.DataFrame:
    # Start with the line containing 'Secondary: Mortality' or Secondary blabla mortality (multiple formats and casings possible)
    result_pq: etree.ElementTree = pdf.pq(':contains("Secondary"):contains("ICU")')

    if len(result_pq) < 3:
        raise Exception('Could not find icu rate!')

    # Define some temporary variables for the iteration loop
    found_values = False
    page = result_pq[1]
    temp = result_pq[2]
    result: Dict[str, str] = {}
    idx_icu = 0

    # Scan line by line until all values are found
    while not found_values:
        temp = temp.getnext()

        # If end of page is reached, go to next page and continue
        if temp is None:
            page = page.getnext()
            if page is None:
                raise Exception('Reached end of file without finding all parameters!')
            temp = page[0]

        if 'number (confidence interval' in temp.xpath("string()"):
            temp = temp.getnext()
            result['icu_medicine'] = temp.xpath("string()")
            temp = temp.getnext()
            result['icu_placebo'] = temp.xpath("string()")
        elif 'Day 29' in temp.xpath("string()"):
            idx_icu = temp.attrib['y0']
        elif idx_icu != 0 and temp.attrib['y0'] == idx_icu:
            if 'icu_medicine' not in result:
                result['icu_medicine'] = temp.xpath("string()")
            else:
                result['icu_placebo'] = temp.xpath("string()")

        if len(result) == 2:
            return pd.DataFrame([result])

def get_mortality_rate(pdf: pdfquery.PDFQuery) -> pd.DataFrame:
    # Start with the line containing 'Secondary: Mortality' or Secondary blabla mortality (multiple formats and casings possible)
    result_pq: etree.ElementTree = pdf.pq(':contains("Secondary"):contains("Mortality")')
    format_type = 1

    if len(result_pq) < 3:
        result_pq = pdf.pq(':contains("Secondary"):contains("mortality")')
        format_type = 2

    if len(result_pq) < 3:
        result_pq = pdf.pq(':contains("Secondary"):contains("COVID-19-related death")')
        format_type = 3

    if len(result_pq) < 3:
        result_pq = pdf.pq(':contains("Serious adverse events")')
        format_type = 4

    if len(result_pq) < 3:
        raise Exception('Could not find mortality')

    print(f'Format type: {format_type}')

    # Define some temporary variables for the iteration loop
    found_values = False
    page = result_pq[1]
    temp = result_pq[2]
    result: Dict[str, str] = {}

    # Variables format 2
    idx_total = 0
    idx_mortality = 0
    # Scan line by line until all values are found
    while not found_values:
        temp = temp.getnext()
        # print(temp.xpath("string()"))
        # If end of page is reached, go to next page and continue
        if temp is None:
            page = page.getnext()
            if page is None:
                raise Exception('Reached end of file without finding all parameters!')
            temp = page[0]

        if format_type == 1:
            if 'Number of subjects analysed' in temp.xpath("string()"):
                temp = temp.getnext()
                result['total_medicine'] = temp.xpath("string()")
                temp = temp.getnext()
                result['total_placebo'] = temp.xpath("string()")
            elif 'number (confidence interval' in temp.xpath("string()"):
                temp = temp.getnext()
                result['mortality_medicine'] = temp.xpath("string()")
                temp = temp.getnext()
                result['mortality_placebo'] = temp.xpath("string()")
            elif 'Day 29' in temp.xpath("string()"):
                idx_mortality = temp.attrib['y0']
            elif idx_mortality != 0 and temp.attrib['y0'] == idx_mortality:
                if 'mortality_medicine' not in result:
                    result['mortality_medicine'] = temp.xpath("string()")
                else:

                    result['mortality_placebo'] = temp.xpath("string()")
        elif format_type == 2:
            if 'Number of subjects analysed' in temp.xpath("string()"):
                idx_total = temp.attrib['y0']
            elif 'Dead' in temp.xpath("string()"):
                idx_mortality = temp.attrib['y0']
            elif idx_total != 0 and temp.attrib['y0'] == idx_total:
                if 'total_medicine' not in result:
                    result['total_medicine'] = temp.xpath("string()")
                else:
                    result['total_placebo'] = temp.xpath("string()")
            elif idx_mortality != 0 and temp.attrib['y0'] == idx_mortality:
                if 'mortality_medicine' not in result:
                    result['mortality_medicine'] = temp.xpath("string()")
                else:
                    result['mortality_placebo'] = temp.xpath("string()")
        elif format_type == 3:
            if 'Number of subjects analysed' in temp.xpath("string()"):
                idx_total = temp.attrib['y0']
            elif 'Units: participants' in temp.xpath("string()"):
                idx_mortality = temp.attrib['y0']
            elif idx_total != 0 and temp.attrib['y0'] == idx_total:
                if 'total_medicine' not in result:
                    result['total_medicine'] = temp.xpath("string()")
                else:
                    result['total_placebo'] = temp.xpath("string()")
            elif idx_mortality != 0 and temp.attrib['y0'] == idx_mortality:
                if 'mortality_medicine' not in result:
                    result['mortality_medicine'] = temp.xpath("string()")
                else:
                    result['mortality_placebo'] = temp.xpath("string()")
        elif format_type == 4:
            if 'subjects affected / exposed' in temp.xpath("string()"):
                idx_total = temp.attrib['y0']
            elif 'number of deaths (all causes)' in temp.xpath("string()"):
                idx_mortality = temp.attrib['y0']
            elif idx_total != 0 and temp.attrib['y0'] == idx_total:
                if 'total_placebo' not in result:
                    result['total_placebo'] = temp.xpath("string()").split('/')[-1].strip().split(' ')[0]
                else:
                    result['total_medicine'] = temp.xpath("string()").split('/')[-1].strip().split(' ')[0]
            elif idx_mortality != 0 and temp.attrib['y0'] == idx_mortality:
                if 'mortality_placebo' not in result:
                    result['mortality_placebo'] = temp.xpath("string()")
                else:
                    result['mortality_medicine'] = temp.xpath("string()")


        if len(result) == 4:
            found_values = True

    return pd.DataFrame([result])

def get_ventilation_rate(pdf: pdfquery.PDFQuery) -> pd.DataFrame:
    # Start with the line containing 'Secondary: Incidence of Mechanical Ventilation by Day 28'
    result_pq: etree.ElementTree = pdf.pq(':contains("Secondary"):contains("Mechanical Ventilation")')
    
    if len(result_pq) < 3:
        raise Exception('Could not find ventilation rate!')

    # Define some temporary variables for the iteration loop
    found_values = False
    page = result_pq[1]
    temp = result_pq[2]
    result: Dict[str, str] = {}
    idx_icu = 0

    # Scan line by line until all values are found
    while not found_values:
        temp = temp.getnext()

        # If end of page is reached, go to next page and continue
        if temp is None:
            page = page.getnext()
            if page is None:
                raise Exception('Reached end of file without finding all parameters!')
            temp = page[0]

        if 'number (confidence interval' in temp.xpath("string()"):
            temp = temp.getnext()
            result['ventilation_medicine'] = temp.xpath("string()")
            temp = temp.getnext()
            result['ventilation_placebo'] = temp.xpath("string()")
        elif 'Day 28' in temp.xpath("string()"):
            idx_icu = temp.attrib['y0']
        elif idx_icu != 0 and temp.attrib['y0'] == idx_icu:
            if 'ventilation_medicine' not in result:
                result['ventilation_medicine'] = temp.xpath("string()")
            else:
                result['ventilation_placebo'] = temp.xpath("string()")

        if len(result) == 2:
            found_values = True
    return pd.DataFrame([result])

def parse_file(pdf_file_path: str) -> Dict[str, pd.DataFrame]:
    log_file = pdf_file_path + '.log'
    
    if os.path.exists(log_file):
        os.remove(log_file)

    pdf = pdfquery.PDFQuery(pdf_file_path)
    pdf.load()

    try:
        mortality_rate = get_mortality_rate(pdf)
    except:
        print(f'Failed parsing mortality rate for {pdf_file_path}, wrote log to {log_file}')
        with open(log_file, 'a+') as f:
            f.write('###### Failed getting mortality rate #####\n')
            f.write(traceback.format_exc())
        mortality_rate = None

    try:
        icu_rate = get_icu_rate(pdf)
    except:
        print(f'Failed parsing icu rate for {pdf_file_path}, wrote log to {log_file}')
        with open(log_file, 'a+') as f:
            f.write('###### Failed getting icu rate #####\n')
            f.write(traceback.format_exc())
        icu_rate = None

    try:
        ventilation_rate = get_ventilation_rate(pdf)
    except:
        print(f'Failed parsing ventilation rate for {pdf_file_path}, wrote log to {log_file}')
        with open(log_file, 'a+') as f:
            f.write('###### Failed getting ventilation rate #####\n')
            f.write(traceback.format_exc())
        ventilation_rate = None

    try:
        global_info = get_global_info(pdf)
    except:
        print(f'Failed parsing global info for {pdf_file_path}, wrote log to {log_file}')
        with open(log_file, 'a+') as f:
            f.write('###### Failed getting global info #####\n')
            f.write(traceback.format_exc())
        global_info = None

    result = dict(
        mortality_rate = mortality_rate,
        icu_rate = icu_rate,
        ventilation_rate = ventilation_rate,
        global_info = global_info,
    )

    # If successful, delete log file if exists to avoid confusion
    return result

def parse_directory(directory: Optional[str] = None,subDir =None) -> Dict[str, pd.DataFrame]:
    # If directory not given, use current directory
    directory = os.path.join(directory,subDir)    

    # Place holder variable for resulting list of dicts of dataframes
    # This list contains 1 dict per pdf file, each dict contains 1 entry per metric (key is the entry name, value is the dataframe)
    global_result: List[Dict[str, pd.DataFrame]] = [] # Contains a list of resulting dataframes
    for pdf_file in [f for f in os.listdir(directory) if f.endswith('.pdf')]:
        print(f'########## {pdf_file} ##########')
        result = parse_file(os.path.join(directory, pdf_file))

        # Skip if failed
        if not result: 
            print(f'no result for {pdf_file}')            
            continue

        # Add filename pdf file name to result set
        for name in result.keys():
            if result[name] is None: continue
            result[name]['file_name'] = pdf_file
        global_result.append(result)

    if not global_result:
        schema_name =f'staging_{subDir.replace("-", "_")}'
        statement = f'''CREATE TABLE if not exists {schema_name}."TRIAL_ENDPOINTS" (
            "index" int8 NULL,
            eudract_number text NULL,
            trial_protocol text NULL,
            trial_end_date timestamp NULL,
            publication_date timestamp NULL,
            file_name text NULL,
            total_medicine text NULL,
            total_placebo text NULL,
            mortality_medicine text NULL,
            mortality_placebo text NULL,
            icu_medicine text NULL,
            icu_placebo text NULL,
            ventilation_medicine text NULL,
            ventilation_placebo text NULL,
            valid_from timestamp NULL
        )'''
        dbInstance.run_statement(statement)
        return None
        
    result = dict(
        mortality_rate = pd.concat([result['mortality_rate'] for result in global_result if result['mortality_rate'] is not None]),
        icu_rate = pd.concat([result['icu_rate'] for result in global_result if result['icu_rate'] is not None]),
        ventilation_rate = pd.concat([result['ventilation_rate'] for result in global_result if result['ventilation_rate'] is not None]),
        global_info = pd.concat([result['global_info'] for result in global_result if result['global_info'] is not None]),
    )
    staging_table = result['global_info']
    staging_table = pd.merge(staging_table, result['mortality_rate'], on='file_name', how='left')
    staging_table = pd.merge(staging_table, result['icu_rate'], on='file_name', how='left')
    staging_table = pd.merge(staging_table, result['ventilation_rate'], on='file_name', how='left')
    staging_table['valid_from'] =  pd.to_datetime(subDir,errors='ignore',format='%d-%m-%Y')
    df=staging_table.drop_duplicates()
    schema_name =f'staging_{subDir.replace("-", "_")}'
    dbInstance.drop_table(f'"{schema_name}"."TRIAL_ENDPOINTS"')
    df.columns = df.columns.str.lower()
    df.to_sql('TRIAL_ENDPOINTS', con, index = True, if_exists='append',schema=schema_name)
    return df