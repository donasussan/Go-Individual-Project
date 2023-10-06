# Data Stream Project

## Overview
This project is a data pipeline designed to ingest data from a CSV file, stream it to Apache Kafka, store it in MySQL, and replicate it to ClickHouse using Debezium and ClickHouse's ReplicatingMergeTree for efficient querying. After setting up and running the pipeline, you can execute SQL queries on the stored data to extract meaningful insights.


## Project Structure
Here's an overview of the project's directory structure:

- `main.go`: The main entry point of the application.
- `api/`: Contains API-related code.
  - `handler.go`: Handles file upload and processing.
- `database/`: Manages database interactions.
  - `database.go`: Responsible for inserting data into MySQL and ClickHouse.
- `routes/`: Defines API routes.
  - `routes.go`: Configures HTTP routes for the API.
- `types/`: Contains custom data types.
  - `types.go`: Defines custom data structures and types.
- `templates/`: Stores HTML templates for the web interface.
  - `home.html`: Home page template.
  - `output.html`: Output page template.
- `static/`: Contains static assets like CSS files.
  - `css/`: Stylesheets.
    - `home.css`: Custom styles for the web interface.
    - `output.css`: Custom styles for the web interface.
- `config/`: Configuration settings for the project.
  - `config.go`: Stores configuration parameters.
- `log/`: Handles project logging.
  - `log.go`: Configures logging for the application.

## Prerequisites
Before you begin, ensure you have met the following requirements:
- Go (Golang) installed
- Apache Kafka, MySQL, and ClickHouse set up and running
- Debezium configured for CDC (Change Data Capture) from MySQL to ClickHouse


## Installation
1. Clone the repository:
   ```bash
   git clone git@github.com:shamnas10/data_stream_repo.git
   cd your-repo
