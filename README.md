# Tourease - Data Engineer

## About Project
Project ini memiliki tujuan untuk melakukan **scrapping data** destinasi wisata (alam, sejarah, dan seni budaya) di seluruh provinsi Indonesia. Setelah data terkumpul kemudian akan diserahkan ke tim terkait yaitu (Backend). Selain itu project ini bertujuan untuk membuat sebuah **data warehouse** untuk memudahkan proses optimasi kueri secara cepat dan efisien melalui proses **ETL (Extract, Transform, Load)**. Dengan melakukan proses pengekstrakan data dari database mysql di AWS Redshift yang kemudian dijadikan sebuah tabel-tabel multi star schema. Data dalam tabel tersebut dilakukan proses Cleaning dan Tranformasi. Kemudian hasilnya akan diupload ke data warehouse (Big Query). Setelah itu dilakukan proses **Visualisasi Data** dari data tersebut menggunakan Looker Studio untuk mendapatkan insight total pengguna, total rute, total admin, total konten video, total destinasi, total destinasi berdasarkan kategori, data rute pengguna, total pengguna dan pengguna baru setiap bulannya.

## Tech Stacks
Daftar tools dan framework yang digunakan dalam project ini:
- Python
- Library in Python: Pandas, mysql.connector, os, pandas-gbq, google.oauth2, google.cloud, sqlalchemy, datetime, json, dotenv, airflow
- Visual Studio Code
- Github
- Jupyter Notebook
- Google Cloud Storage
- Dbeaver
- Big Query
- Looker Studio
- MySQL
- Apache Airflow
- CSV
- Draw.io
- Google Maps
- Tiktok
- Youtube
- Reels Instagram
- other

## Project Structure
Berikut adalah struktur direktori dan deskripsi singkat dari masing-masing file dan folder dalam proyek ini:

```plaintext
DE_CAPSTONE/
├── dags/
│   ├── etl_destination_tourease_dag.py    # DAG untuk proses ETL destinasi
│   └── etl_routes_tourease_dag.py         # DAG untuk proses ETL rute
├── data_final_destination/                # Folder untuk data hasil merged data scrapping
├── data_scrapping/                        # Folder untuk data hasil scrapping
├── .env                                   # File konfigurasi environment
├── .env-example                           # Contoh file konfigurasi environment
├── .gitignore                             # File untuk mengecualikan file tertentu dari Git
├── accountKey.json                        # File kunci akun untuk akses Google Cloud
├── etl_pipeline_destinations.ipynb        # Notebook Jupyter untuk pipeline ETL destinasi
├── etl_pipeline_routes.ipynb              # Notebook Jupyter untuk pipeline ETL rute
├── merged_data_scrapping.ipynb            # Notebook Jupyter untuk proses penggabungan data scrapping
├── README.md                              # Dokumentasi proyek
├── SustainTour-Data-Scraping-Flow.png     # Diagram arsitektur scrapping data
└── SustainTour-ETL-Architecture.png       # Diagram arsitektur ETL
```

## Architecture Scrapping Data (HLA)
 ![Scrapping Data Diagram](https://github.com/Capstone-Project-Sustain-Tour-Group-3/de-capstone/blob/main/SustainTour-Data-Scraping-Flow.png?raw=true)

 ## Architecture ETL (HLA)
 ![ETL Diagram](https://github.com/Capstone-Project-Sustain-Tour-Group-3/de-capstone/blob/main/SustainTour-ETL-Architecture.png?raw=true)

 ## Usage
 ### Prerequisites
 - Install Python 3.x
 - Install dependencies from `Library in python`
 - Set up environment variables as shown in `.env-example`
 - Configure Google Cloud credentials using `accountKey.json`

 ### Running ETL Pipelines
 1. **Setup Airflow:**
    - Initialize Airflow database: `airflow db init`
    - Start Airflow web server: `airflow webserver --port 8080`
    - Start Airflow scheduler: `airflow scheduler`
 2. **Trigger DAGs:**
    - Access Airflow web interface at `http://localhost:8080`
    - Trigger the DAGs `etl_destination_tourease_dag` and `etl_routes_tourease_dag`

 ### Running Jupyter Notebooks
 - Open Jupyter notebooks `etl_pipeline_destinations.ipynb`, `etl_pipeline_routes.ipynb`, and `merged_data_scrapping.ipynb` to run and test individual ETL processes.

 ## Data Visualization
 - Use **Looker Studio** to visualize data stored in **Big Query**.
 - Create dashboards for insights on total pengguna, total rute, total admin, total konten video, total destinasi, total destinasi berdasarkan kategori, data rute pengguna, total pengguna dan pengguna baru setiap bulannya.

 ## Access Looker Studio
 - To access the Looker Studio dashboards, visit: [Dashboard Tourease](https://lookerstudio.google.com/reporting/011c2bb3-691e-4480-b5a5-3159830c9676/page/ZTA3D)
 - Make sure to have appropriate access permissions to view the dashboards.

 ## Contributions
 Feel free to fork this repository and create a pull request with your changes. Make sure to follow the code style and add appropriate tests.

 ## License
 This project is licensed under the MIT License.
 ```
 This structure and content should give a comprehensive overview and guide to using and understanding the project.
 ```