from pydantic import BaseModel

class PredictionInput(BaseModel):
    city: str
    year :int
    total_quantity: int
    last_12m :int
    last_6m :int
    last_3m :int
    lag_1 :int
    lag_2 :int
    lag_3 :int


class PredictionOutput(BaseModel):
    forecast: float




