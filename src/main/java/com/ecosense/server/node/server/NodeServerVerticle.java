package com.ecosense.server.node.server;

import com.ecosense.server.utils.Logger;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.core.datagram.DatagramSocket;

import static com.ecosense.server.database.SensorVerticle.SENSOR_SERVICE_ADDRESS;
import static com.ecosense.server.utils.Utils.action;

public class NodeServerVerticle extends AbstractVerticle implements Logger {
  @Override
  public void start(Future<Void> future) {

    DatagramSocket datagramSocket = vertx.createDatagramSocket();

    datagramSocket.rxListen(8080, "0.0.0.0")
      .map(this::handleSocket)
      .ignoreElement()
      .subscribe(() -> {
        logger().info("Node Server Verticle deployed");
        future.complete();
      }, error -> future.fail("Failed to deploy Node Server Verticle"));
  }

  private DatagramSocket handleSocket(DatagramSocket socket) {
    return socket.handler(handler -> {
      JsonObject jsonObject = new JsonObject(handler.data().toString());
      String type = jsonObject.getString("type");
      switch (type) {
        case "registration":
          jsonObject.put("action", "save");
          vertx.eventBus().rxSend(SENSOR_SERVICE_ADDRESS, jsonObject, action("save"))
            .ignoreElement()
            .subscribe(() ->
                logger().info("New sensor added to the database"),
              error ->
                logger().error("Couldn't add sensor", error));
          break;
        case "warning":
          logger().info(buildWarningMessage(jsonObject));
          break;
      }
    });
  }

  private String buildWarningMessage(JsonObject jsonObject) {
    return "WARNING ALERT:" + jsonObject.getString("warning");
  }
}

