
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

from anomaly_detectors.utils.data_handler import  *
from anomaly_detectors.utils.preprocessors import *
from anomaly_detectors.utils.error_codes import error_codes
from anomaly_detectors.utils import type_checker as type_checker
from anomaly_detectors.utils import csv_prep_for_reader as csv_helper
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

def main(filepath,thres_prob=0.5,samples_to_wait=10,expected_run_length=100,to_plot=True):

        '''
        Wrapper function which should be called inorder to run the anomaly detection, it has four parts :
        
        *reader           - Class Data_reader defined in data_handler.py which takes in filepath as string as 
                            input and groups the dataframe into list of dataframes, incase of any error it 
                            returns dictionary with error message
        *preprocessor     - preprocessors are defined in preprocessors.py, which takes in data and gives out processed 
                            data
        *anomaly detector - Class Bayesian_Changept_Detector defined in bayesian_changept_detector.py, which takes in
                            data and algorithm parameters as argument and returns anomaly indexes and data.        
        *make_ack_json    - Function to make acknowledge json
        
        
        Arguments :
        
        Required Parameter:
            reader_kwargs: the Json object in the format of the input json given from reader api
        Optional Parameter:
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
                       
            
            # type_checker is python file which has Type_checker class which checks given parameter types
            checker = type_checker.Type_checker(kwargs=algo_kwargs,ideal_args_type=algo_params_type)
            # res is None when no error raised, otherwise it stores the appropriate error message
            res = checker.params_checker()
            if(res!=None):
                return json.dumps(res)
            
            
            # instanstiating the reader class with reader arguments
            data_reader = Data_reader(filepath=filepath)
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
                        print("\nAnomaly detection for AssetNo : {} ,Metric : {}\n ".format(
                            assetno,data_per_asset.columns[data_col]))
                        
                        anomaly_detector = bayesian_changept_detector.Bayesian_Changept_Detector(data_per_asset,
                                                                                                 assetno=assetno,
                                                                                                 **algo_kwargs)
                        data,anom_indexes = anomaly_detector.detect_anomalies()

                        anomaly_detectors.append(anomaly_detector)
                        
                ack_json = {}
                ack_json = make_ackg_json.make_ack_json(anomaly_detectors)
                        
                return json.dumps(ack_json)
            elif(type(entire_data)==dict):
                return json.dumps(entire_data)
            else:
                '''
                Data empty errors
                '''
                return json.dumps(error_codes1['data_missing'])
        except Exception as e:
            '''
            unknown exceptions are caught here and traceback used to know the source of the error
            '''
            traceback.print_exc()
            
            error_codes1['unknown']['message']=str(e)
            return json.dumps(error_codes1['unknown'])