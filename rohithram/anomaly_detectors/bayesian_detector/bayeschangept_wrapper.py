
'''
importing all the required header files
'''

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.pylab import rcParams
import datetime as dt
import time
import os


# Importing reader,and other dependencies python files as modules
from anomaly_detectors.reader_writer import db_properties as db_props
from anomaly_detectors.reader_writer import writer_configs as write_args


from anomaly_detectors.utils.preprocessors import *
from anomaly_detectors.utils.data_handler import *
from anomaly_detectors.utils.error_codes import error_codes
from anomaly_detectors.utils import type_checker as type_checker
from anomaly_detectors.utils import csv_prep_for_reader as csv_helper
from anomaly_detectors.utils import reader_helper
from anomaly_detectors.utils import make_ackg_json
from anomaly_detectors.bayesian_detector import bayesian_changept_detector

import json
import traceback
import warnings
warnings.filterwarnings('ignore')

rcParams['figure.figsize'] = 12, 9
rcParams[ 'axes.grid']=True
'''
ideal argument types for algorithm
'''
algo_params_type ={
            'data_col_index':int,
            'pthres':float or int,
            'Nw':int,
            'mean_runlen':int,
            'to_plot':bool
        }

mode_options = ['detect only','detect and log','log only']

def main(json_data,mode=mode_options[0],thres_prob=0.5,samples_to_wait=10,expected_run_length=100,to_plot=True):

        '''
        Wrapper function which should be called inorder to run the anomaly detection, it has four parts :
        *reader           - Class Data_reader defined in data_handler.py which takes in json string as input and parses json 
                            and gives list of dataframes, incase of any error it returns dictionary with error message
        *preprocessor     - preprocessors are defined in preprocessors.py, which takes in data and gives out processed 
                            data
        *anomaly detector - Class Bayesian_Changept_Detector defined in bayesian_changept_detector.py, which takes in
                            data and algorithm parameters as argument and returns anomaly indexes and data.        
        *make_ack_json    - Function to make acknowledge json
        *writer           - Class Postgres_Writer defined in data_handler.py which takes in anomaly detector object and
                            and sql_queries , db_properties and table name as args and gives out response code.
        
        Arguments :
        
        Required Parameter:
            json_data: the Json object in the format of the input json given from reader api
        Optional Parameter:
            mode      : It's by default 'detect only' option, takes in mode_options which is in string format
            thres_prob (Type : Float , between 0 and 1) It is the probability threshold after which points are considered as change points
            Default: 0.5
            samples_to_wait: Positive Integer representing the no of samples after which the run length probability will be calculated. It is one of the algorithm related parameter
            Default: 10
            expected_run_length: positive Integer that is average run length or no of samples between two change-points. This originates from modeling the interval between change-points with exponential distribution
            Default : 100
            to_plot : Boolean .Give True to see the plots of change-points detected and False if there is no need for plotting
            Default : True
        Note:
        To run this, import this python file as module and call this function with required args and it will detect
        anomalies and writes to the local database.
        This algorithm is univariate, so each metric per asset is processed individually
        '''
        
        

        #algorithm arguments
        algo_kwargs={
            'data_col_index':1,
            'pthres':thres_prob,
            'Nw':samples_to_wait,
            'mean_runlen':expected_run_length,
            'to_plot':to_plot
        }
              
        '''
            #reseting the error_codes to avoid overwritting
            #error_codes is a python file imported as error_codes which has error_codes dictionary mapping 
        '''
        error_codes1 = error_codes()
        
        try: 
            
            #converting user given mode to all lower case
            mode = mode.lower()
            # Check for mode
            if(mode not in mode_options):
                error_codes1['param']['data']['argument']='mode'
                error_codes1['param']['data']['value']=mode
                error_codes1['param']['message']='should be one of {}'.format((mode_options))
                return json.dumps(error_codes1['param'])
            
            
            # type_checker is python file which has Type_checker class which checks given parameter types
            checker = type_checker.Type_checker(kwargs=algo_kwargs,ideal_args_type=algo_params_type)
            # res is None when no error raised, otherwise it stores the appropriate error message
            res = checker.params_checker()
            if(res!=None):
                return json.dumps(res)
            
            # instanstiating the reader class with reader arguments
            data_reader = Data_reader(json_data=json_data)
            #getting list of dataframes per asset if not empty
            #otherwise gives string 'Empty Dataframe'
            entire_data = data_reader.read()
            writer_data = []
            anomaly_detectors = []
            if((len(entire_data)!=0 and entire_data!=None and type(entire_data)!=dict)):

                '''
                looping over the data per assets and inside that looping over metrics per asset
                * Instantiates anomaly detector class with algo args and metric index to detect on
                * Stores the anomaly indexes and anomaly detector object to bulk write to db at once
                '''

                for i,data_per_asset in enumerate(entire_data):
                    assetno = pd.unique(data_per_asset['assetno'])[0]
                    data_per_asset[data_per_asset.columns[1:]] = normalise_standardise(data_per_asset[data_per_asset.columns[1:]])
                    print("Overview of data : \n{}\n".format(data_per_asset.head()))

                    for data_col in range(1,len(data_per_asset.columns[1:])+1):
                        algo_kwargs['data_col_index'] = data_col
                        print("\nAnomaly detection for AssetNo : {} , Metric : {}\n ".format(assetno,
                                                                                             data_per_asset.columns[data_col]))
                        
                        
                        anomaly_detector = bayesian_changept_detector.Bayesian_Changept_Detector(data_per_asset,
                                                                                                 assetno=assetno,
                                                                                                 **algo_kwargs)
                        data,anom_indexes = anomaly_detector.detect_anomalies()
                        
                            
                        sql_query_args = write_args.writer_kwargs
                        table_name = write_args.table_name
                        window_size = 10

                        anomaly_detectors.append(anomaly_detector)
                        
                
                
                ack_json = {}
                
                if(mode==mode_options[0] or mode==mode_options[1]):
                    ack_json = make_ackg_json.make_ack_json(anomaly_detectors)

                if(mode==mode_options[1] or mode==mode_options[2]):
                    '''
                    Instantiates writer class to write into local database with arguments given below
                    Used for Bulk writing
                    '''
                    writer = Postgres_Writer(anomaly_detectors,db_credentials=db_props.db_connection,sql_query_args=sql_query_args,
                                            table_name=table_name,window_size=window_size)

                    #called for mapping args before writing into db
                    res = writer.map_outputs_and_write()
                    if(res!=error_codes1['success']):
                        return json.dumps(res)
                
                #This case for only log mode, if writer is success , it returns success msg
                # Checking for ack_json to be empty or not
                if(bool(ack_json)==False):
                    ack_json['header'] = error_codes1['success']
                    
                return json.dumps(ack_json)
            elif(type(entire_data)==dict):
                return json.dumps(entire_data)
            else:
                '''
                Data empty error
                '''
                return json.dumps(error_codes1['data_missing'])
        except Exception as e:
            '''
            unknown exceptions are caught here and traceback used to know the source of the error
            '''
            traceback.print_exc()
            
            error_codes1['unknown']['message']=str(e)
            return json.dumps(error_codes1['unknown'])