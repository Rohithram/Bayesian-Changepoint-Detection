

from anomaly_detectors.utils.error_codes import error_codes
from anomaly_detectors.reader_writer import reader_new as reader
import traceback
import json



def read(reader_kwargs):
        '''
        Function to read the data using reader api, and parses the json to list of dataframes per asset
        '''
        response_json = ''
        try:
            response_json=reader.reader_api(**reader_kwargs)

            return response_json
        except Exception as e:
            traceback.print_exc()
            return str(response_json)