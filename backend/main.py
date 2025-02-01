from fastapi import FastAPI, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from app.analysis.text_analysis import analyze_news_articles
import pandas as pd
import os

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.post("/api/analyze")
async def analyze_text(file: UploadFile = File(...)):
    # Create temporary directory if it doesn't exist
    os.makedirs("temp", exist_ok=True)
    
    # Save uploaded file
    temp_path = f"temp/{file.filename}"
    with open(temp_path, "wb") as buffer:
        content = await file.read()
        buffer.write(content)
    
    # Read and analyze the file
    try:
        df = pd.read_csv(temp_path)
        results = analyze_news_articles(df)
        return results
    finally:
        # Cleanup
        if os.path.exists(temp_path):
            os.remove(temp_path)