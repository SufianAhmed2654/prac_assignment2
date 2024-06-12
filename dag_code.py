from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
import re
import csv
import json
import os


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'data',
    default_args=default_args,
    description='Extract data and set up DVC with Git',
    schedule='@once',  
)
def extract_dawn(data_dir):
    print("Starting extract_dawn function...")
    url = 'https://www.dawn.com/'

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        print(f"Page retrieved successfully. Status code: {response.status_code}")
        soup = BeautifulSoup(response.text, 'html.parser')
        
        story_links = soup.find_all('a', class_='story__link')
        
        with open(os.path.join(data_dir, 'dawn_news_articles.csv'), mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(['Title', 'URL', 'Description', 'Excerpt'])
            
            for link in story_links:
                href = link.get('href')
                title = link.get_text(strip=True)
                print(f"Processing link: {title} - {href}")
                if href and re.search(r'^https://www\.dawn\.com.*news', href):
                    parent_article = link.find_parent('article')
                    if parent_article:
                        story_div = parent_article.find('div', class_=re.compile(r'^story__excerpt'))
                        excerpt = story_div.get_text(strip=True) if story_div else 'N/A'
                    else:
                        excerpt = 'N/A'
                    
                    article_response = requests.get(href, headers=headers)
                    
                    if article_response.status_code == 200:
                        article_soup = BeautifulSoup(article_response.text, 'html.parser')
                        
                        first_paragraph = article_soup.find('p')
                        
                        if first_paragraph:
                            writer.writerow([title, href, first_paragraph.get_text(strip=True), excerpt]) 
                        else:
                            print(f"No description found for article: {href}")
    else:
        print(f"Failed to retrieve the page. Status code: {response.status_code}")

def fetch_article_paragraph(url):
    print(f"Fetching article paragraph for URL: {url}")
    response = requests.get(url)
    print(f"Page retrieved successfully. Status code: {response.status_code}")
    response.raise_for_status()
    soup = BeautifulSoup(response.text, 'html.parser')
    
    script_tag = soup.find('script', id='__NEXT_DATA__')
    if not script_tag:
        print(f"No JSON script tag found for URL: {url}")
        return ""

    json_data = json.loads(script_tag.string)
    
    try:
        contents = json_data['props']['pageProps']['page']
        article_key = list(contents.keys())[0]
        blocks = contents[article_key]['contents']
        for block in blocks:
            if block['type'] == 'text':
                paragraphs = block['model']['blocks']
                for paragraph in paragraphs:
                    if paragraph['type'] == 'paragraph':
                        first_paragraph = paragraph['model']['text']
                        return first_paragraph
    except (KeyError, IndexError):
        print(f"Error navigating JSON data: {KeyError} - {IndexError}")
        return ""
    
    return ""    

def extract_bbc(data_dir):
    print("Starting extract_bbc function...")
    url = 'https://www.bbc.com'

    response = requests.get(url)
    response.raise_for_status() 
    print(f"Page retrieved successfully. Status code: {response.status_code}")
    soup = BeautifulSoup(response.text, 'html.parser')

    article_divs = soup.find_all('div', {'data-testid': ['edinburgh-card', 'westminster-card', 'manchester-card', 'windsor-card', 'chester-card']})
    articles_data = []

    for div in article_divs:
        try:
            article_link_tag = div.find('a')
            if article_link_tag:
                href = article_link_tag.get('href')
                if href and href.startswith('/'):
                    full_url = url + href
                else:
                    full_url = href
                if full_url.find("live") == -1 and not full_url.startswith("https://cloud.email.bbc.com/") and full_url.find("videos") == -1:
                    title = article_link_tag.get_text(strip=True)
                    print(f"Processing article: {title} - {full_url}")
                    excerpt_tag = div.find('p', {'data-testid': 'card-description'})
                    if excerpt_tag:
                        excerpt = excerpt_tag.get_text(strip=True) 
                        first_paragraph=fetch_article_paragraph(full_url)
                    else:
                        excerpt = "N/A"
                        first_paragraph = fetch_article_paragraph(full_url)
                    articles_data.append({
                        'Title': title,
                        'URL': full_url,
                        'Excerpt': excerpt,
                        'Paragraph': first_paragraph
                    })

        except AttributeError:
            continue  

    with open(os.path.join(data_dir, 'bbc_articles.csv'), mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=['Title', 'URL', 'Excerpt', 'Paragraph'])
        writer.writeheader()  
        for article in articles_data:
            writer.writerow(article)

def extract_data(**kwargs):
    print("Starting extract_data function...")
    ti = kwargs['ti']
    cwd = ti.xcom_pull(task_ids='print_cwd')
    
    assignment2_dir = os.path.join(cwd, 'assignment2')
    if not os.path.exists(assignment2_dir):
        os.makedirs(assignment2_dir)
        print(f"Created directory: {assignment2_dir}")
    else:
        print(f"Directory already exists: {assignment2_dir}")

    os.chdir(assignment2_dir)
    print(f"Changed working directory to: {assignment2_dir}")

    data_dir = os.path.join(assignment2_dir, 'data')
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
        print(f"Created directory: {data_dir}")
    else:
        print(f"Directory already exists: {data_dir}")
    extract_dawn(data_dir)
    extract_bbc(data_dir)
    
    print("Completed extract_data function.")
    if not os.path.exists(os.path.join(data_dir, 'bbc_articles.csv')):
        print("File bbc_articles.csv does not exist in data directory.")
    if not os.path.exists(os.path.join(data_dir, 'dawn_news_articles.csv')):
        print("File dawn_news_articles.csv does not exist in data directory.")
        
def print_working_directory(**kwargs):
    cwd = os.getcwd()
    print(f"Current working directory: {cwd}")
    return cwd
print_cwd_task = PythonOperator(
    task_id='print_cwd',
    python_callable=print_working_directory,
    provide_context=True,
    dag=dag,
)
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    provide_context=True,
    dag=dag,
)

dvc_commands = """
cd {{ ti.xcom_pull(task_ids="print_cwd") }}
echo "Current working directory: $(pwd)"
echo "Listing all files and directories:"
ls -l
ls -l assignment2
cd assignment2
echo "Current working directory: $(pwd)"
cp /home/sufian/airflow/dags/dag_code.py .
echo "Initializing Git repository..."
git init
dvc init
git config user.email "i202654@nu.edu.pk"
git config user.name "SufianAhmed2654"
git commit -m "dvc commit"
dvc add data/bbc_articles.csv data/dawn_news_articles.csv
git add data/dawn_news_articles.csv.dvc data/.gitignore data/bbc_articles.csv.dvc dag_code.py
git commit -m "dvc commit 2"
dvc remote add -d storage gdrive://1sW4tVChibnF_sJ4ZYaYoLY8FqdTmWIVn
git commit .dvc/config -m "Configure remote storage"
dvc push
git remote add origin https://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@github.com/SufianAhmed2654/prac_assignment2.git
git branch -M main
git push -u origin main
"""

dvc_task = BashOperator(
    task_id='run_dvc_commands',
    bash_command=dvc_commands,
    dag=dag,
)

print_cwd_task>>extract_task >>  dvc_task
