import sql from "./shared/sql.ts";

async function generateReport() {
  let topShifts = await sql`
WITH ranked_shifts AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY shift_week ORDER BY shift_total DESC) AS row_num
    FROM dbt.shifts_model
)
SELECT
    shift_id,
    taxi_id,
	shift_week,
    shift_total,
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
        "Taxi ID (truncated)":
          shift["taxi_id"].slice(0, 6) +
          "..." +
          shift["taxi_id"].slice(shift["taxi_id"].length - 6),
        "Total earnings": shift["shift_total"],
        "Trip count": shift["trip_count"],
        "Total miles": shift["shift_miles"],
        "Total seconds": shift["shift_seconds"],
      }))
    );
  }

  process.exit(0);
}

generateReport();
