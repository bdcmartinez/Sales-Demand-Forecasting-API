import joblib
from pathlib import Path
import pandas as pd

class SalesModel:
    def __init__(self):
        model_path = Path("/home/bdcmartinez/Desktop/Projects/ML_END_TO_END/model/model.pkl")

        self.model = joblib.load(model_path)

    def predict(self, data):

                
        data_dict = data.dict()     
        df = pd.DataFrame([data_dict])   
        prediction = self.model.predict(df)  
        return float(prediction[0])

