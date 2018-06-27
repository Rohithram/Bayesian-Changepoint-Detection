

import pandas as pd
import numpy as np
from anomaly_detectors.utils import error_codes

def make_ack_json(anomaly_detectors):
    
    bad_response = {"code":"204","status" : "No Content","message": "Input Data is Empty"}
    no_anom_response = {"code":"200","status" : "OK","message": "No Anomalies detected"}
    
    ack_json = lambda:{"header":'',"body":[]}
    anom_per_asset  = lambda:{"asset": "<asset_serial_number>","anomalies":[],"anom_counts":{}}
    anom_per_metric = lambda:{ "name":"<TagName>","datapoints":[]}
    
    ack_json1 = ack_json()
    
    if(anomaly_detectors[0].algo_type=='univariate'):
        
        no_assets = pd.unique([anomaly_detector.assetno for anomaly_detector in anomaly_detectors]).size 
        anomaly_detectors_per_asset = np.split(np.array(anomaly_detectors),no_assets)
        
        for i in range(no_assets):
            
            anom_per_asset1 = anom_per_asset()
            for anomaly_detector in anomaly_detectors_per_asset[i]:
                
                data = anomaly_detector.data
                anom_indexes = anomaly_detector.anom_indexes

                if(len(data[anomaly_detector.metric_name])!=0):
                    ack_json1['header'] = error_codes.error_codes['success']
                    anom_per_asset1['asset'] = anomaly_detector.assetno
                    anom_per_asset1['anom_counts'][anomaly_detector.metric_name] = len(anom_indexes)
                    
                    anom_per_metric1 = anom_per_metric()
                    anom_per_metric1['name'] = anomaly_detector.metric_name
                    anom_timevalues = data.index[anom_indexes]
                    anom_codes = [anomaly_detector.algo_code for i in range(len(anom_timevalues))]

                    anom_timestamps = list(zip(anom_timevalues,anom_timevalues,anom_timevalues,anom_codes))
                    anom_per_metric1['datapoints'] = [dict(zip(['from_timestamp','to_timestamp','anomaly_timestamp','anomaly_code'],anom_timestamp)) for anom_timestamp in anom_timestamps]                    

                    anom_per_asset1['anomalies'].append(anom_per_metric1)
                
                else:
                    ack_json1['header'] = bad_response
                    return ack_json1
                    
            ack_json1['body'].append(anom_per_asset1)

            
    else:
        
        for anomaly_detector in anomaly_detectors:

            data = anomaly_detector.data
            anom_indexes = anomaly_detector.anom_indexes

            if(len(data)!=0):
                ack_json1['header'] = error_codes.error_codes['success']
                anom_per_asset1 = anom_per_asset()
                anom_per_asset1['asset'] = anomaly_detector.assetno
                anom_per_asset1['anom_counts'][anomaly_detector.metric_name] = len(anom_indexes)

                metric_names = anomaly_detector.metric_name

                for metric_name in metric_names:
                    anom_per_metric1 = anom_per_metric()
                    anom_per_metric1['name'] = metric_name
                    anom_timevalues = data.index[anom_indexes]
                    anom_codes = [anomaly_detector.algo_code for i in range(len(anom_timevalues))]
                    anom_timestamps = list(zip(anom_timevalues,anom_timevalues,anom_timevalues,anom_codes))
                    
                    anom_per_metric1['datapoints'] = [dict(zip(['from_timestamp','to_timestamp','anomaly_timestamp','anomaly_code'],anom_timestamp)) for anom_timestamp in anom_timestamps]                    
                    anom_per_asset1['anomalies'].append(anom_per_metric1)

                ack_json1['body'].append(anom_per_asset1)
            else:
                ack_json1['header'] = bad_response
            
        
    return ack_json1