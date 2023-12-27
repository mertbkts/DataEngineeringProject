print('Starting')
print('2')
import pandas as pd
print('3')

# Define file path
file_path = '/app/small_file_aa.json'

# Define the range of rows to be loaded
start_row = 10
end_row = 15

# Initialize variables
chunks = []
current_row = 0

# Use chunksize to control memory usage. Adjust this based on the size of your rows.
chunksize = 1

# Read the JSON file in chunks
for chunk in pd.read_json(file_path, lines=True, chunksize=chunksize):
    # Update the current row index
    current_row += chunksize

    # Check if the current chunk is within the desired row range
    if current_row >= start_row and current_row <= end_row:
        # Add the chunk to the list
        chunks.append(chunk)

    # Break the loop if the end row has been reached
    if current_row >= end_row:
        break

# Concatenate all relevant chunks into a single DataFrame
df = pd.concat(chunks, ignore_index=True)
print(df)
print('4')

#drop short titles
df = df[df['title'].str.split().str.len() >= 2]

# Drop rows with empty authors
df[df['authors'].notnull() & (df['authors'] == '')]


df = df.drop('abstract', axis=1)

#drop rows where doi is empty
df = df[df['doi'].notnull()]
print('18')


import aiohttp
import asyncio
import pandas as pd
import json
import os
import nest_asyncio

# Apply the necessary patch for asyncio in Jupyter
nest_asyncio.apply()

# Function to make asynchronous API calls
async def fetch(session, url):
    try:
        async with session.get(url) as response:
            if response.status == 200:
                return await response.text()
            else:
                return None
    except Exception as e:
        print(f"Error fetching URL {url}: {e}")
        return None

# Asynchronous batch processing function
async def fetch_all(dois):
    async with aiohttp.ClientSession() as session:
        tasks = []
        for doi in dois:
            url = f"https://api.crossref.org/works/{doi}"
            tasks.append(fetch(session, url))
        responses = await asyncio.gather(*tasks)
        return responses



import requests
def fetch_semantic_scholar_data_by_doi(doi):
    url = f"https://api.semanticscholar.org/v1/paper/{doi}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        logging.error(f"Error fetching data from Semantic Scholar: {response.status_code}")
        return None

def extract_needed_info(doi):
    fields_needed = ['publisher', 'published-print', 'published-online', 'reference-count', 'type', 'subject']
    extracted_info = {field: None for field in fields_needed}

    semantic_scholar_data = fetch_semantic_scholar_data_by_doi(doi)

    if not semantic_scholar_data:
        logging.info(f"No data found for DOI '{doi}' in Semantic Scholar.")
        return extracted_info

    # Map Semantic Scholar data to the desired fields
    extracted_info['publisher'] = semantic_scholar_data.get('venue')
    extracted_info['published-print'] = semantic_scholar_data.get('year')
    extracted_info['type'] = semantic_scholar_data.get('fieldsOfStudy', [None])[0]
    extracted_info['reference-count'] = semantic_scholar_data.get('citationCount')
    extracted_info['subject'] = ', '.join(semantic_scholar_data.get('fieldsOfStudy', []))

    # Semantic Scholar does not provide a direct mapping for 'published-online' and 'publication_type'
    extracted_info['published-online'] = None
    extracted_info['publication_type'] = None

    return extracted_info


import warnings
import logging
warnings.filterwarnings('ignore')

# Check for additional fields and add them if not present
additional_fields = ['publisher', 'published-print', 'published-online', 'reference-count', 'type', 'bibtex', 'subject', 'publication_type']
for field in additional_fields:
    if field not in df.columns:
        df[field] = None

# Ensure logging is configured
logging.basicConfig(level=logging.INFO)

# Iterate over DataFrame rows
for index, row in df.iterrows():
    if pd.notna(row['doi']):
        # Extract needed information for each DOI
        additional_info = extract_needed_info(row['doi'])

        # Update DataFrame with the additional information
        for key, value in additional_info.items():
            df.at[index, key] = value



# df1 = df.sample(1000, random_state=10)
df1 = df
print(df1)


df1['publication_type'].value_counts()

import requests
import pandas as pd

def get_crossref_info(doi, session):
    if pd.notna(doi):
        url = f"https://api.crossref.org/works/{doi}"
        try:
            response = session.get(url)
            if response.status_code == 200:
                data = response.json()['message']
                authors = data.get('author', [])
                titles = data.get('title', [])
                author_names = ', '.join([f"{a.get('given', '')} {a.get('family', '')}".strip() for a in authors])
                publication_title = titles[0] if titles else None
                return author_names, publication_title
            else:
                return None, None
        except requests.RequestException:
            return None, None
    else:
        return None, None

def update_df(df):
    updated_df = df.copy()
    with requests.Session() as session:
        for index, row in updated_df.iterrows():
            try:
                author_info, publication_info = get_crossref_info(row['doi'], session)
                updated_df.at[index, 'refined_authors'] = author_info
                updated_df.at[index, 'refined_publication_title'] = publication_info
                print(f"Processed row {index + 1}/{len(updated_df)}")
            except Exception as e:
                print(f"Error processing row {index}: {e}")
    return updated_df

df1 = update_df(df1)

print('zyx')
print(df1)

import pandas as pd
import requests
import xml.etree.ElementTree as ET

def query_dblp_for_venue(title):
    url = f"https://dblp.org/search/publ/api?q={requests.utils.quote(title)}&format=xml"
    try:
        response = requests.get(url, timeout=5)  # 5-second timeout
        if response.status_code == 200:
            root = ET.fromstring(response.content)
            venue_info = root.find(".//hit/info/venue")
            return venue_info.text if venue_info is not None else 'Unknown'
        else:
            return 'Unknown'
    except requests.RequestException:
        return 'Unknown'


# Iterate over DataFrame rows
for index, row in df1.iterrows():
    if pd.notna(row['title']):
        venue_info = query_dblp_for_venue(row['title'])
        df1.at[index, 'resolved_venue'] = venue_info


import pandas as pd
import requests


# Define a function to infer gender for a given name using the Genderize.io API
def infer_gender(name):
    url = f"https://api.genderize.io?name={name}"
    
    response = requests.get(url)
    if response.status_code == 200:
        result = response.json()
        return result.get("gender", "unknown")
    else:
        return "unknown"
print(df1.columns)

def process_authors(authors):
    # Handle None values
    if authors is None:
        return 'unknown'

    # Split the author string by comma to get individual names
    author_list = authors.split(", ")
    gendered_authors = []

    for author in author_list:
        # Check if the author's name is not empty and contains a space
        if author and " " in author:
            # Extract first name (assuming first word is the first name)
            first_name = author.split()[0]
            # Infer gender for the first name
            gender = infer_gender(first_name)
        else:
            # Default to 'unknown' if the author's name is not in the expected format
            gender = "unknown"

        # Append the author name with gender
        gendered_authors.append(f"{author} ({gender})")

    return ", ".join(gendered_authors)


# Apply the function to the 'refined_authors' column
df1['authors_with_gender'] = df1['refined_authors'].apply(process_authors)


def map_category_to_field_of_study(category):
    category_map = {
        'astro-ph': "Astronomy and Astrophysics",
        'hep-': "High Energy Physics",
        'quant-ph': "Quantum Physics",
        'cond-mat': "Condensed Matter Physics",
        'math.': "Mathematics and Computer Science",
        'stat.': "Statistics",
        'cs.': "Computer Science",
        'nucl-': "Nuclear Physics",
        'gr-qc': "General Relativity and Quantum Cosmology",
        'nlin.': "Nonlinear Sciences",
        'physics.': "General Physics",
        'q-bio': "Quantitative Biology",
        'q-fin': "Quantitative Finance",
        'bio-': "Biology",
        'chem-': "Chemistry",
        'econ-': "Economics",
        'eng-': "Engineering",
        'env-': "Environmental Science",
        'geo-': "Geosciences",
        'soc-': "Social Sciences",
        'med-': "Medical Sciences",
        'psy-': "Psychology",
        'edu-': "Education"
    }
    for key, field in category_map.items():
        if key in category:
            return field
    return "Other"



# Apply the function to create a new column
df1['field_of_study'] = df1['categories'].apply(map_category_to_field_of_study)


def get_crossref_publication_info_extended(doi):
    """
    Query Crossref API to get extended publication information based on DOI.
    """
    url = f"https://api.crossref.org/works/{doi}"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()['message']

            # Extract additional information
            publication_year = data.get('published-print', data.get('published-online', {})).get('date-parts', [[None]])[0][0]
            journal_name = data.get('container-title', [None])[0]
            volume = data.get('volume', None)
            issue = data.get('issue', None)
            pages = data.get('page', None)

            return publication_year, journal_name, volume, issue, pages
        else:
            return None, None, None, None, None
    except requests.RequestException:
        return None, None, None, None, None



# Iterate over DataFrame rows
for index, row in df1.iterrows():
    if pd.notna(row['doi']):
        # Get extended publication info
        pub_year, journal, volume, issue, pages = get_crossref_publication_info_extended(row['doi'])

        # Update DataFrame with the new information
        df1.at[index, 'publication_year'] = pub_year
        df1.at[index, 'journal_name'] = journal
        df1.at[index, 'volume'] = volume
        df1.at[index, 'issue'] = issue
        df1.at[index, 'pages'] = pages
    print(index)



df1['publication_year'] = df1['publication_year'].astype('Int64')

df1 = df1.drop_duplicates(subset='doi', keep='first')

print(df1)
# df1 = pd.read_csv('/app/updated_data_new.csv')
print(df1.shape)


from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

# Load environment variables
# load_dotenv()

host = os.getenv("DB_HOST", "localhost")  # Default to 'localhost' if not set
user = os.getenv("DB_USER", "default_user")  # Default user if not set
pwd = os.getenv("DB_PASSWORD", "default_password")  # Default password if not set
db = os.getenv("DB_NAME", "default_db")  # Default database name if not set
print(f'postgresql://{user}:{pwd}@{host}:5432/{db}')

# Connect to the PostgreSQL database
engine = create_engine(f'postgresql://{user}:{pwd}@{host}:5432/{db}', echo=True)

new_db_name = "dwh"

# Use a connection with autocommit mode to create the new database
try:
    # Check if the database already exists
    db_exists = engine.execute(text(f"SELECT 1 FROM pg_database WHERE datname='{new_db_name}'")).scalar()
    if not db_exists:
        # The database does not exist, create a new one
        engine.execute(text(f"CREATE DATABASE {new_db_name}"))
        print(f"Database '{new_db_name}' created successfully")
    else:
        print(f"Database '{new_db_name}' already exists")
except Exception as e:
    print(f"An error occurred while creating the database: {e}")




create_tables_query = '''
-- Create Dimension Tables
CREATE TABLE IF NOT EXISTS dim_submitter (
    submitter_id SERIAL PRIMARY KEY,
    submitter_name VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS dim_authors (
    author_id SERIAL PRIMARY KEY,
    author_name VARCHAR(255),
    gender VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS dim_title (
    title_id SERIAL PRIMARY KEY,
    title TEXT,
    refined_publication_title TEXT
);

CREATE TABLE IF NOT EXISTS dim_journal_ref (
    journal_ref_id SERIAL PRIMARY KEY,
    journal_ref VARCHAR(255),
    resolved_venue VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS dim_license (
    license_id SERIAL PRIMARY KEY,
    license_type VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS dim_field_of_study (
    field_of_study_id SERIAL PRIMARY KEY,
    field_name VARCHAR(255)
);

-- Create Fact Table
CREATE TABLE IF NOT EXISTS fact_publications (
    publication_id VARCHAR(255) PRIMARY KEY,
    submitter_id INT REFERENCES dim_submitter(submitter_id),
    title_id INT REFERENCES dim_title(title_id),
    doi VARCHAR(255),
    report_no VARCHAR(255),
    comments TEXT,
    publication_type VARCHAR(255),
    journal_ref_id INT REFERENCES dim_journal_ref(journal_ref_id),
    license_id INT REFERENCES dim_license(license_id),
    field_of_study_id INT REFERENCES dim_field_of_study(field_of_study_id),
    publication_year INT,  -- New column for publication year
    journal_name VARCHAR(255),  -- New column for journal name
    volume VARCHAR(50),  -- New column for volume
    issue VARCHAR(50),  -- New column for issue
    pages VARCHAR(100)  -- New column for pages
);

-- Create Junction Table for Many-to-Many Relationship between Publications and Authors
CREATE TABLE IF NOT EXISTS publication_author_junction (
    publication_id VARCHAR(255),
    author_id INT,
    PRIMARY KEY (publication_id, author_id),
    FOREIGN KEY (publication_id) REFERENCES fact_publications(publication_id),
    FOREIGN KEY (author_id) REFERENCES dim_authors(author_id)
);

'''


from sqlalchemy import create_engine, text, MetaData
import os
from dotenv import load_dotenv

try:
    with engine.connect() as conn:
        conn.execute(text(create_tables_query))
        conn.execute(text("COMMIT;"))  # Explicit commit
    print("Tables created successfully")
except Exception as e:
    print(f"An error occurred: {e}")




# Query to list all tables in the database
tables_query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
tables = pd.read_sql(tables_query, engine)
print("Tables in the database:", tables['table_name'].tolist())


def get_or_create_primary_key(value, table_name, column_name, primary_key_column, connection):
    if pd.isna(value):
        return None

    # Check if the value already exists and return its primary key
    select_query = text(f"SELECT {primary_key_column} FROM {table_name} WHERE {column_name} = :val")
    result = connection.execute(select_query, {'val': value}).fetchone()
    if result:
        return result[0]

    # If not, insert the new value and return its primary key
    insert_query = text(f"INSERT INTO {table_name} ({column_name}) VALUES (:val) RETURNING {primary_key_column}")
    result = connection.execute(insert_query, {'val': value}).fetchone()
    return result[0]

def get_or_create_author(author_name, gender, connection):
    if pd.isna(author_name):
        return None

    # Check if the author already exists
    select_query = text("SELECT author_id FROM dim_authors WHERE author_name = :name")
    result = connection.execute(select_query, {'name': author_name}).fetchone()
    if result:
        return result[0]

    # If not, insert the new author with gender and return the author_id
    insert_query = text("INSERT INTO dim_authors (author_name, gender) VALUES (:name, :gender) RETURNING author_id")
    result = connection.execute(insert_query, {'name': author_name, 'gender': gender}).fetchone()
    return result[0]

def populate_tables(df, engine):
    with engine.connect() as connection:
        for index, row in df.iterrows():
            try:
                # Transaction starts
                with connection.begin():
                    # Check if DOI already exists in the database
                    doi_check_query = text("SELECT 1 FROM fact_publications WHERE publication_id = :doi")
                    exists = connection.execute(doi_check_query, {'doi': row['doi']}).fetchone()
                    
                    if not exists:
                        print('???? New row')
                        # Get or create primary keys for the dimension tables
                        submitter_id = get_or_create_primary_key(row['submitter'], 'dim_submitter', 'submitter_name', 'submitter_id', connection)
                        title_id = get_or_create_primary_key(row['title'], 'dim_title', 'title', 'title_id', connection)
                        journal_ref_id = get_or_create_primary_key(row['journal-ref'], 'dim_journal_ref', 'journal_ref', 'journal_ref_id', connection)
                        license_id = get_or_create_primary_key(row['license'], 'dim_license', 'license_type', 'license_id', connection)
                        field_of_study_id = get_or_create_primary_key(row['field_of_study'], 'dim_field_of_study', 'field_name', 'field_of_study_id', connection)

                        # Construct a new row for fact_publications table
                        fact_row = {
                            'publication_id': row['doi'],
                            'submitter_id': submitter_id,
                            'title_id': title_id,
                            'doi': row['doi'],
                            'report_no': row.get('report-no', None),
                            'comments': row['comments'],
                            'publication_type': row.get('publication_type', None),
                            'journal_ref_id': journal_ref_id,
                            'license_id': license_id,
                            'field_of_study_id': field_of_study_id,
                            'publication_year': row.get('publication_year', None),
                            'journal_name': row.get('journal_name', None),
                            'volume': row.get('volume', None),
                            'issue': row.get('issue', None),
                            'pages': row.get('pages', None)
                        }
                        df_fact = pd.DataFrame([fact_row])
                        df_fact.to_sql('fact_publications', connection, if_exists='append', index=False)

                        # Handle authors and populate publication_author_junction
                        if pd.notna(row['authors_with_gender']):
                            for author_with_gender in row['authors_with_gender'].split(', '):
                                # Extract author name and gender
                                parts = author_with_gender.rsplit(' (', 1)
                                author_name = parts[0].strip()
                                gender = parts[1].rstrip(')') if len(parts) > 1 else None
                                author_id = get_or_create_author(author_name, gender, connection)

                                # Check if combination of publication_id and author_id already exists
                                junction_check_query = text("SELECT 1 FROM publication_author_junction WHERE publication_id = :pub_id AND author_id = :auth_id")
                                junction_exists = connection.execute(junction_check_query, {'pub_id': row['doi'], 'auth_id': author_id}).fetchone()

                                if not junction_exists:
                                    # Insert into junction table if combination does not exist
                                    junction_row = {'publication_id': row['doi'], 'author_id': author_id}
                                    df_junction = pd.DataFrame([junction_row])
                                    df_junction.to_sql('publication_author_junction', connection, if_exists='append', index=False)

            except Exception as e:
                print(f"Error processing row {index}: {e}")

# Assuming df1 is your DataFrame
populate_tables(df1, engine)


# Assuming df1 is your DataFrame
populate_tables(df1, engine)

# # Get a glimpse of each table
# for table in tables['table_name']:
#     query = f'SELECT * FROM public."{table}" LIMIT 5'
#     data = pd.read_sql(query, engine)
#     print(f"\nTable: {table}")
#     print(data)

# Close the database connection
engine.dispose()
print('end')


# # In[107]:


# # Create a database connection
# engine = create_engine(f'postgresql://{user}:{pwd}@{host}:5434/{db}')

# # Get a glimpse of each table
# for table in tables['table_name']:
#     query = f'SELECT * FROM public."{table}" LIMIT 5'
#     data = pd.read_sql(query, engine)
#     print(f"\nTable: {table}")
#     print(data)


# # ## Questions

# # In[108]:


# # Get detailed information for each publication, including a list of authors
# query = '''
# SELECT 
#     fact_publications.publication_id, 
#     dim_title.title, 
#     STRING_AGG(dim_authors.author_name, ', ') AS authors,
#     dim_field_of_study.field_name AS field_of_study,
#     fact_publications.publication_year,
#     dim_journal_ref.journal_ref,
#     fact_publications.journal_name,
#     fact_publications.volume,
#     fact_publications.issue,
#     fact_publications.pages
# FROM public."fact_publications"
# JOIN public."publication_author_junction" ON fact_publications.publication_id = publication_author_junction.publication_id
# JOIN public."dim_authors" ON publication_author_junction.author_id = dim_authors.author_id
# JOIN public."dim_title" ON fact_publications.title_id = dim_title.title_id
# JOIN public."dim_field_of_study" ON fact_publications.field_of_study_id = dim_field_of_study.field_of_study_id
# LEFT JOIN public."dim_journal_ref" ON fact_publications.journal_ref_id = dim_journal_ref.journal_ref_id
# GROUP BY fact_publications.publication_id, dim_title.title, dim_field_of_study.field_name, dim_journal_ref.journal_ref
# ORDER BY fact_publications.publication_id;
# '''
# pd.read_sql(query, engine)


# # In[133]:


# # How many publications were there each year?
# query = '''
# SELECT publication_year, COUNT(*) AS publication_count
# FROM public."fact_publications"
# WHERE publication_year IS NOT NULL
# GROUP BY publication_year
# ORDER BY publication_year;
# '''
# pd.read_sql(query, engine)


# # In[134]:


# # Who are the top 5 most frequent publishers (submitter)?
# query = '''
# SELECT dim_submitter.submitter_name, COUNT(*) AS publication_count
# FROM public."fact_publications"
# JOIN public."dim_submitter" ON fact_publications.submitter_id = dim_submitter.submitter_id
# GROUP BY dim_submitter.submitter_name
# ORDER BY publication_count DESC
# LIMIT 5;
# '''
# pd.read_sql(query, engine)


# # In[135]:


# # Which publications are in the field of 'High Energy Physics'?
# query = '''
# SELECT fact_publications.publication_id, dim_title.title
# FROM public."fact_publications"
# JOIN public."dim_field_of_study" ON fact_publications.field_of_study_id = dim_field_of_study.field_of_study_id
# JOIN public."dim_title" ON fact_publications.title_id = dim_title.title_id
# WHERE dim_field_of_study.field_name = 'High Energy Physics';
# '''
# pd.read_sql(query, engine)


# # In[136]:


# # Which authors have the most publications?
# query = '''
# SELECT dim_authors.author_name, COUNT(*) AS publication_count
# FROM public."publication_author_junction"
# JOIN public."dim_authors" ON publication_author_junction.author_id = dim_authors.author_id
# GROUP BY dim_authors.author_name
# ORDER BY publication_count DESC;
# '''
# pd.read_sql(query, engine)


# # In[137]:


# # How many publications do not have a specified license?
# query = '''
# SELECT COUNT(*) AS publications_without_license
# FROM public."fact_publications"
# WHERE license_id IS NULL;
# '''
# pd.read_sql(query, engine)


# # In[138]:


# # How many publications are there in each journal?
# query = '''
# SELECT journal_name, COUNT(*) AS publication_count
# FROM public."fact_publications"
# WHERE journal_name IS NOT NULL
# GROUP BY journal_name
# ORDER BY publication_count DESC;
# '''
# pd.read_sql(query, engine)


# # In[139]:


# # How many publications are there for each license type?
# query = '''
# SELECT dim_license.license_type, COUNT(*) AS publication_count
# FROM public."fact_publications"
# JOIN public."dim_license" ON fact_publications.license_id = dim_license.license_id
# GROUP BY dim_license.license_type;
# '''
# pd.read_sql(query, engine)


# # In[140]:


# # Who are the top authors in each field of study?
# query = '''
# SELECT dim_field_of_study.field_name, dim_authors.author_name, COUNT(publication_author_junction.author_id) AS publication_count
# FROM public."publication_author_junction"
# JOIN public."fact_publications" ON publication_author_junction.publication_id = fact_publications.publication_id
# JOIN public."dim_authors" ON publication_author_junction.author_id = dim_authors.author_id
# JOIN public."dim_field_of_study" ON fact_publications.field_of_study_id = dim_field_of_study.field_of_study_id
# GROUP BY dim_field_of_study.field_name, dim_authors.author_name
# ORDER BY dim_field_of_study.field_name, publication_count DESC;
# '''
# pd.read_sql(query, engine)


# # In[141]:


# # How has the number of publications in each field of study changed over the years?
# query = '''
# SELECT publication_year, dim_field_of_study.field_name, COUNT(*) AS publication_count
# FROM public."fact_publications"
# JOIN public."dim_field_of_study" ON fact_publications.field_of_study_id = dim_field_of_study.field_of_study_id
# WHERE publication_year IS NOT NULL
# GROUP BY publication_year, dim_field_of_study.field_name
# ORDER BY publication_year, dim_field_of_study.field_name;
# '''
# pd.read_sql(query, engine)


# # In[142]:


# # Which pairs of authors have collaborated the most?
# query = '''
# SELECT A.author_name AS Author1, B.author_name AS Author2, COUNT(*) AS collaboration_count
# FROM public."publication_author_junction" PAJ1
# JOIN public."publication_author_junction" PAJ2 ON PAJ1.publication_id = PAJ2.publication_id AND PAJ1.author_id < PAJ2.author_id
# JOIN public."dim_authors" A ON PAJ1.author_id = A.author_id
# JOIN public."dim_authors" B ON PAJ2.author_id = B.author_id
# GROUP BY A.author_name, B.author_name
# ORDER BY collaboration_count DESC;
# '''
# pd.read_sql(query, engine)


# # In[143]:


# # Which publications have the highest number of authors?
# query = '''
# SELECT fact_publications.publication_id, COUNT(publication_author_junction.author_id) AS author_count
# FROM public."fact_publications"
# JOIN public."publication_author_junction" ON fact_publications.publication_id = publication_author_junction.publication_id
# GROUP BY fact_publications.publication_id
# ORDER BY author_count DESC
# LIMIT 10;
# '''
# pd.read_sql(query, engine)


# # In[144]:


# # What is the average length of comments in each field of study?
# query = '''
# SELECT dim_field_of_study.field_name, AVG(LENGTH(fact_publications.comments)) AS avg_comment_length
# FROM public."fact_publications"
# JOIN public."dim_field_of_study" ON fact_publications.field_of_study_id = dim_field_of_study.field_of_study_id
# GROUP BY dim_field_of_study.field_name;
# '''
# pd.read_sql(query, engine)


# # In[145]:


# # Which publications are missing journal, volume, issue, or pages information?
# query = '''
# SELECT publication_id, journal_name, volume, issue, pages
# FROM public."fact_publications"
# WHERE journal_name IS NULL OR volume IS NULL OR issue IS NULL OR pages IS NULL;
# '''
# pd.read_sql(query, engine)


# # In[146]:


# # How are each submitter's publications spread across different fields of study?
# query = '''
# SELECT dim_submitter.submitter_name, dim_field_of_study.field_name, COUNT(*) AS publication_count
# FROM public."fact_publications"
# JOIN public."dim_submitter" ON fact_publications.submitter_id = dim_submitter.submitter_id
# JOIN public."dim_field_of_study" ON fact_publications.field_of_study_id = dim_field_of_study.field_of_study_id
# GROUP BY dim_submitter.submitter_name, dim_field_of_study.field_name
# ORDER BY dim_submitter.submitter_name, publication_count DESC;
# '''
# pd.read_sql(query, engine)


# # In[153]:


# # Analyze the gender diversity in publications
# query = '''
# SELECT fact_publications.publication_id, COUNT(DISTINCT CASE WHEN dim_authors.gender = 'male' THEN publication_author_junction.author_id END) AS male_authors,
# COUNT(DISTINCT CASE WHEN dim_authors.gender = 'female' THEN publication_author_junction.author_id END) AS female_authors
# FROM public."fact_publications"
# JOIN public."publication_author_junction" ON fact_publications.publication_id = publication_author_junction.publication_id
# JOIN public."dim_authors" ON publication_author_junction.author_id = dim_authors.author_id
# GROUP BY fact_publications.publication_id;
# '''
# pd.read_sql(query, engine)


# # In[154]:


# # What are the most referenced journals in the publications?
# query = '''
# SELECT dim_journal_ref.journal_ref, COUNT(*) AS reference_count
# FROM public."fact_publications"
# JOIN public."dim_journal_ref" ON fact_publications.journal_ref_id = dim_journal_ref.journal_ref_id
# GROUP BY dim_journal_ref.journal_ref
# ORDER BY reference_count DESC;
# '''
# pd.read_sql(query, engine)


# # In[149]:


# # How has the use of different licenses changed over time?
# query = '''
# SELECT publication_year, dim_license.license_type, COUNT(*) AS license_count
# FROM public."fact_publications"
# JOIN public."dim_license" ON fact_publications.license_id = dim_license.license_id
# GROUP BY publication_year, dim_license.license_type
# ORDER BY publication_year, dim_license.license_type;
# '''
# pd.read_sql(query, engine)


# # In[150]:


# # Find the most common author collaborations in the field of 'Quantum Physics'
# query = '''
# SELECT 
#     A.author_name AS Author1, 
#     B.author_name AS Author2, 
#     COUNT(*) AS collaboration_count
# FROM public."publication_author_junction" PAJ1
# JOIN public."publication_author_junction" PAJ2 ON PAJ1.publication_id = PAJ2.publication_id AND PAJ1.author_id < PAJ2.author_id
# JOIN public."dim_authors" A ON PAJ1.author_id = A.author_id
# JOIN public."dim_authors" B ON PAJ2.author_id = B.author_id
# JOIN public."fact_publications" ON PAJ1.publication_id = fact_publications.publication_id
# JOIN public."dim_field_of_study" ON fact_publications.field_of_study_id = dim_field_of_study.field_of_study_id
# WHERE dim_field_of_study.field_name = 'Quantum Physics'
# GROUP BY A.author_name, B.author_name
# ORDER BY collaboration_count DESC;
# '''
# pd.read_sql(query, engine)


# # In[151]:


# # Distribution of publication types (journal, conference, etc.) across different fields of study
# query = '''
# SELECT 
#     fact_publications.publication_type, 
#     dim_field_of_study.field_name, 
#     COUNT(*) AS count
# FROM public."fact_publications"
# JOIN public."dim_field_of_study" ON fact_publications.field_of_study_id = dim_field_of_study.field_of_study_id
# WHERE fact_publications.publication_type IS NOT NULL
# GROUP BY fact_publications.publication_type, dim_field_of_study.field_name
# ORDER BY fact_publications.publication_type, dim_field_of_study.field_name;
# '''
# pd.read_sql(query, engine)


# # In[152]:


# # Analyze the gender diversity of authors in each field of study
# query = '''
# SELECT 
#     dim_field_of_study.field_name, 
#     COUNT(DISTINCT CASE WHEN dim_authors.gender = 'male' THEN publication_author_junction.author_id END) AS male_authors,
#     COUNT(DISTINCT CASE WHEN dim_authors.gender = 'female' THEN publication_author_junction.author_id END) AS female_authors
# FROM public."fact_publications"
# JOIN public."publication_author_junction" ON fact_publications.publication_id = publication_author_junction.publication_id
# JOIN public."dim_authors" ON publication_author_junction.author_id = dim_authors.author_id
# JOIN public."dim_field_of_study" ON fact_publications.field_of_study_id = dim_field_of_study.field_of_study_id
# GROUP BY dim_field_of_study.field_name;
# '''
# pd.read_sql(query, engine)


# # In[ ]:





# # In[ ]:




