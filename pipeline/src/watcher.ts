import chokidar from "chokidar";

import logger from "./shared/logger.ts";
import config from "./shared/config.ts";
import queues from "./shared/queues.ts";

async function startWatching() {
  logger.info(`Watching ${config.DATA_INTAKE} for new data`);

  const watcher = chokidar.watch(config.DATA_INTAKE, {
    // Keep watching
    persistent: true,
    // Wait until large files are finished writing before triggering callbacks
    awaitWriteFinish: true,
    // Ignore processed files
    ignored: (path) => {
      return path.endsWith(".staged.csv") || path.endsWith(".failed.csv");
    },
  });

  watcher.on("add", async (path) => {
    logger.info(`File detected: ${path}`);
    await queues.newData.send({ path });
  });
}

startWatching();
