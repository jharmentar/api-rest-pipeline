# API Pipeline - E-commerce Data

Pipeline that consumes data from the e-commerce API, transforms it, and saves it partitioned.

## Setup

1. Clone the repo
2. Create a `.env` file with your token:
   ```
   API_TOKEN = your_token_here
   API_BASE_URL = your_url_here
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

```bash
python main.py
```
## Docker
docker build -t api-pipeline .
docker run api-pipeline

## Output

The data is saved in `output/` partitioned by year and month:
```
output/
├── orders/
│   ├── order_year=2024/
│   │   ├── order_month=2024-01/
│   │   ├── order_month=2024-02/
│   │   └── ...
└── orders_all.parquet
```

## Author
[Josue Armenta] - [2026-01-01]
