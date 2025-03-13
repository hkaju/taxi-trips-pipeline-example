import PgBoss from "pg-boss";

import config from "./config.ts";
import logger from "./logger.ts";

const boss = await (async () => {
  const boss = new PgBoss(
    `postgres://${config.POSTGRES_USER}:${config.POSTGRES_PASSWORD}@${config.POSTGRES_HOST}:5432/${config.POSTGRES_DB}`
  );
  boss.on("error", logger.error);

  await boss.start();

  return boss;
})();

// Helper function to bundle up queue operations and ensure type safety
async function queue<T extends object>(name: string) {
  await boss.createQueue(name);

  return {
    name,
    send: async (data: T) => await boss.send(name, data),
    work: async (workFn: PgBoss.WorkHandler<T>) => {
      logger.child({ worker: name }).info("Waiting for jobs");
      return await boss.work(name, workFn);
    },
  };
}

export default {
  newData: await queue<{ path: string }>("new-data"),
  stagedData: await queue<{ table: string }>("staged-data"),
  cleanedData: await queue<{ table: string }>("cleaned-data"),
  loadedData: await queue<{ table: string }>("loaded-data"),
};
