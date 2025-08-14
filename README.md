# DataCompose Demo

A demonstration project showcasing data processing and email cleaning using PySpark with Docker Compose orchestration.

## Features

- **Email Data Cleaning**: Advanced email validation and cleaning using PySpark
- **Docker Compose Setup**: Complete containerized Spark cluster environment
- **Email Primitives**: Reusable components for email processing
- **Jupyter Integration**: Interactive notebooks for data exploration

## Project Structure

```
datacompose-demo/
├── docker-compose.yaml     # Docker Compose configuration for Spark cluster
├── Dockerfile.dev          # Development container configuration
├── datacompose.json        # DataCompose configuration
├── notebooks/              # Jupyter notebooks for data processing
│   └── email_cleaning.py   # Email cleaning implementation
└── build/                  # Python modules
    └── clean_emails/       # Email cleaning modules
        └── email_primitives.py  # Email processing primitives
```

## Prerequisites

- Docker and Docker Compose
- Python 3.8+
- 4GB+ RAM recommended

## Quick Start

1. **Clone the repository**
   ```bash
   git clone https://github.com/yourusername/datacompose-demo.git
   cd datacompose-demo
   ```

2. **Start the Spark cluster**
   ```bash
   docker-compose up -d
   ```

3. **Access Jupyter Notebook**
   - Open http://localhost:8888 in your browser
   - Navigate to the `notebooks` directory

4. **Run the email cleaning demo**
   - Open `email_cleaning.py` in Jupyter
   - Execute cells to see email validation and cleaning in action

## Email Cleaning Features

The demo includes sophisticated email processing capabilities:

- **Validation**: Detect invalid email formats
- **Typo Correction**: Fix common domain typos (gmail, yahoo, hotmail)
- **Normalization**: Handle Gmail dots and plus addressing
- **Duplicate Detection**: Identify duplicate emails across providers
- **Data Quality**: Generate quality scores and statistics

## Docker Services

The `docker-compose.yaml` configures:
- Spark Master node
- Spark Worker nodes
- Jupyter Notebook server
- Shared volume for data persistence

## Development

### Running in Development Mode

```bash
docker-compose -f docker-compose.yaml up --build
```

### Accessing Spark UI

- Spark Master: http://localhost:8080
- Spark Worker: http://localhost:8081

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

MIT License - feel free to use this demo for learning and development.

## Support

For questions or issues, please open an issue in the GitHub repository.