import postgres from "postgres";

import config from "./config.ts";

export default postgres(
  `postgres://${config.POSTGRES_USER}:${config.POSTGRES_PASSWORD}@${config.POSTGRES_HOST}:5432/${config.POSTGRES_DB}`
);
