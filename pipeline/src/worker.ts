import fs from "fs";
import crypto from "crypto";
import { Transform } from "node:stream";
import { pipeline } from "node:stream/promises";

import sql from "./shared/sql.ts";
import logger from "./shared/logger.ts";
import queues from "./shared/queues.ts";

/*******************
 * Queue: new-data *
 *******************/

await queues.newData.work(async ([job]) => {
  const jobLogger = logger.child({
    worker: queues.newData.name,
    job: job.id,
  });

  const { path } = job.data;
  jobLogger.info(`Staging ${path}`);

  const batchId = crypto.randomUUID().slice(0, 6);
  const table = "pipeline.trips_staging_" + batchId;

  try {
    await sql`CREATE TABLE ${sql(
      table
    )} (LIKE pipeline.trips_staging_template);`;

    await pipeline(
      fs.createReadStream(path),
      appendValue(path),
      await sql`COPY ${sql(table)} FROM STDIN CSV HEADER`.writable()
    );

    fs.renameSync(path, path.replace(".csv", ".staged.csv"));

    // TODO: Include number of staged rows in the logs
    jobLogger.info(`Staged ${path} into ${table}`);
    await queues.stagedData.send({ table });
  } catch (error) {
    fs.renameSync(path, path.replace(".csv", ".failed.csv"));
    jobLogger.error(error, `Failed to stage ${path}`);
  }
});

function appendValue(value: string): Transform {
  return new Transform({
    transform: function (data, _, callback) {
      this.push(data.toString().replaceAll("\n", `,${value}\n`));
      callback();
    },
  });
}

/**********************
 * Queue: staged-data *
 **********************/

await queues.stagedData.work(async ([job]) => {
  const jobLogger = logger.child({
    worker: queues.stagedData.name,
    job: job.id,
  });

  const { table } = job.data;
  jobLogger.info(`Cleaning ${table}`);

  try {
    await sql`
    WITH bad_data AS (
      DELETE FROM ${sql(table)}
      WHERE
        trip_id IS NULL
        OR taxi_id IS NULL
        OR trip_start IS NULL
        OR trip_end IS NULL
        OR trip_end::timestamp < trip_start::timestamp
      RETURNING *
    )
    INSERT INTO pipeline.bad_trips SELECT * from bad_data;`;

    // TODO: Include number of cleaned rows in the logs
    jobLogger.info(`Cleaned ${table}`);
    await queues.cleanedData.send({ table });
  } catch (error) {
    jobLogger.error(error, `Failed to clean ${table}`);
  }
});

/***********************
 * Queue: cleaned-data *
 ***********************/

await queues.cleanedData.work(async ([job]) => {
  const jobLogger = logger.child({
    worker: queues.cleanedData.name,
    job: job.id,
  });

  const { table } = job.data;
  const mainTable = "pipeline.trips";
  jobLogger.info(`Loading ${table} into ${mainTable}`);

  try {
    await sql`
        INSERT INTO ${sql(mainTable)}
        SELECT
            trip_id,
            taxi_id,
            trip_start::timestamp,
            trip_end::timestamp,
            tsrange(trip_start::timestamp, trip_end::timestamp + interval '1 hour') as trip_duration,
            trip_seconds::int,
            trip_miles::float,
            pickup_census_tract::bigint,
            dropoff_census_tract::bigint,
            pickup_community_area::int,
            dropoff_community_area::int,
            fare::float,
            tips::float,
            tolls::float,
            extras::float,
            trip_total::float,
            payment_type,
            company,
            pickup_centroid_latitude::float,
            pickup_centroid_longitude::float,
            pickup_centroid_location,
            dropoff_centroid_latitude::float,
            dropoff_centroid_longitude::float,
            dropoff_centroid_location
        FROM
            ${sql(table)}
        ON CONFLICT DO NOTHING;`;

    // TODO: Include number of loaded rows in the logs
    jobLogger.info(`Loaded ${table} into ${mainTable}`);
    await queues.loadedData.send({ table });
  } catch (error) {
    jobLogger.error(error, `Failed to load ${table}`);
  }
});

/**********************
 * Queue: loaded-data *
 **********************/

await queues.loadedData.work(async ([job]) => {
  const jobLogger = logger.child({
    worker: queues.loadedData.name,
    job: job.id,
  });

  const { table } = job.data;
  jobLogger.info(`Dropping ${table}`);

  try {
    await sql`DROP TABLE ${sql(table)};`;
    jobLogger.info(`Dropped ${table}`);
  } catch (error) {
    jobLogger.error(error, `Failed to drop ${table}`);
  }
});
