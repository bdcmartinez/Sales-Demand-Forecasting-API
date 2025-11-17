

from fastapi import FastAPI 
from app.api import predict

app = FastAPI(title='Sales Status', version="1.0.0")

app.include_router(predict.router, prefix="/predict", tags=["Prediction"])
