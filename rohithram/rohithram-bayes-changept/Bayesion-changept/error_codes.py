
def reset():
    error_codes = {'success':{"code" : "200","status" : "OK"},
                'param':{'code':'400','status':'Bad Request',"message" : "Invalid arguments","data" : {}},
               'db':{'code':'441','status':'Database error',"message" : "Unable to communicate with database"},
               'unknown':{"code" : "500","status" : "Unknown Exception"},
                'data_missing':{"code" : "204","status" : "No Content","message": "Input Data is Empty"}              
              }

error_codes = {
                'success':{"code" : "200","status" : "OK"},
                'param':{'code':'400','status':'Bad Request',"message" : "Invalid arguments","data" : {}},
               'db':{'code':'441','status':'Database error',"message" : "Unable to communicate with database"},
               'unknown':{"code" : "500","status" : "Unknown Exception"},
                'data_missing':{"code" : "204","status" : "No Content","message": "Input Data is Empty"}              
              }