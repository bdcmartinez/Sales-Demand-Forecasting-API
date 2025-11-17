

from fastapi import APIRouter, Depends, Query, HTTPException
from fastapi.responses import JSONResponse
from app.schemas.prediction import PredictionInput, PredictionOutput
from app.models.model import SalesModel
from typing import List

router = APIRouter()

@router.post("/by_month", response_model=PredictionOutput)
def get_sales(
    params: PredictionInput
    
):
    model = SalesModel()
    
    result = model.predict(data=params)
    return PredictionOutput(forecast=result)




