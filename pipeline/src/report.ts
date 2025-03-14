import sql from "./shared/sql.ts";

async function generateReport() {
  let topShifts = await sql`
WITH ranked_shifts AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY date_trunc('week', lower(shift_duration)) ORDER BY shift_total DESC) AS row_num
    FROM dbt.shifts
)
SELECT
    shift_id,
    taxi_id,
	  date_trunc('week', lower(shift_duration)) as shift_week,
    shift_total,
    shift_fare,
    shift_tolls,
    shift_tips,
    shift_extras,
    shift_miles,
    shift_seconds::float,
    trip_count::float
FROM 
    ranked_shifts
WHERE 
    row_num <= 10;
`;

  const weeklyShifts = Object.groupBy(
    topShifts,
    ({ shift_week }) => shift_week
  );

  for (const week of Object.keys(weeklyShifts)) {
    console.log(`\n\nWeek of ${week}`);
    console.table(
      weeklyShifts[week].map((shift) => ({
        "Taxi ID":
          shift["taxi_id"].slice(0, 6) +
          "..." +
          shift["taxi_id"].slice(shift["taxi_id"].length - 6),
        "Trip count": shift["trip_count"],
        Fares: shift["shift_fare"],
        Tips: shift["shift_tips"],
        Tolls: shift["shift_tolls"],
        Extras: shift["shift_extras"],
        Total: shift["shift_total"],
        "Total miles": shift["shift_miles"],
        "Total seconds": shift["shift_seconds"],
      }))
    );
  }

  process.exit(0);
}

generateReport();
