FROM python:3.9

WORKDIR /app

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy the Python script and JSON file into the image
COPY data_enrichment.py .
COPY small_file_aa.json .
COPY updated_data_new.csv .

CMD ["python", "./data_enrichment.py"]