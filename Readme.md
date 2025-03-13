# Taxi Trips Chicago 2024 data pipeline

## Usage

To start the data pipeline, run:

```
docker compose up
```

Wait until all the services come up and weekly data files have appeared in the `data` directory.

### Processing data

To start ingesting data, move CSV file(s) to the `data-intake` folder.
The pipeline will automatically detect and process new files from there.

Once a file is successfully loaded into the database, it will be renamed with a `.staged.csv` extension.
If loading fails, the data file will be renamed with a `.failed.csv` extension.

To re-ingest a data file, just rename it and remove `.staged` or `.failed`.

### Updating `dbt` models

To update the `shifts` model, you can run:

```
docker compose run dbt run
```

The model is updated incrementally so the updates are faster if this command is run after each new batch is ingested.

This command will also execute before each report is generated so that the data returned always reflects the latest state of the database.

### Viewing reports

To see a report of the highest-earning taxi shifts per week based on the latest data, run:

```
docker compose run report
```

### Re-download data

To repopulate the weekly taxi trip CSV files in `data`, you can run:

```
docker compose run downloader
```

## Bottlenecks & scalability

The file watcher service is currently designed to run only a single instance but since its only job is to watch a folder for new files and publish messages, it should be fairly performant.

Pipeline workers can be scaled up or down as needed for throughput.
Each worker services all the queues in the pipeline but for larger data volumes it will probably be necessary to pool and scale them per queue.

The slowest step in the ingestion pipeline seems to be loading cleaned data from a staging table into the final partitioned `trips` table.
