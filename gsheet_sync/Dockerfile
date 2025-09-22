# gsheet_sync/Dockerfile
FROM python:3.12-slim

# 1) System basics (optional but nice)
ENV PYTHONUNBUFFERED=1 PIP_DISABLE_PIP_VERSION_CHECK=1

# 2) Install Python deps
# psycopg2-binary avoids needing libpq/build tools in the image
RUN pip install --no-cache-dir \
    SQLAlchemy==2.0.31 \
    pandas==2.2.2 \
    psycopg2-binary==2.9.9 \
    gspread==6.1.4 \
    gspread-dataframe==3.3.1 \
    python-dotenv==1.0.1

# 3) Create app dir
WORKDIR /app

# 4) Copy your script into the image
# (Assumes pg_to_gsheet.py is in the same directory as this Dockerfile,
#  or adjust the path accordingly.)
COPY pg_to_gsheet.py /app/pg_to_gsheet.py

# 6) Run the sync
CMD ["python", "/app/pg_to_gsheet.py"]