import logger from "./src/config/logger";
import config from "config";
import { createMessageBroker } from "./src/factories/broker-factory";
import { MessageBroker } from "./src/types/broker";
import ws from "./src/socket";

const startServer = async () => {
  let broker: MessageBroker | null = null;
  try {
    broker = createMessageBroker();
    await broker.connectConsumer();
    await broker.consumeMessage(["order"], false);

    const PORT = config.get("server.port");
    ws.wsServer
      .listen(PORT, () => {
        console.log(`Listening on port ${PORT}`);
      })
      .on("error", (err) => {
        console.log("err", err.message);
        process.exit(1);
      });
  } catch (err) {
    logger.error("Error happened: ", err);
    if (broker) {
      await broker.disconnectConsumer();
    }
    process.exit(1);
  }
};

void startServer();
