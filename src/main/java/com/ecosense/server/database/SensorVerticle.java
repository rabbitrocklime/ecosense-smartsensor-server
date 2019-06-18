package com.ecosense.server.database;

import com.ecosense.server.utils.Logger;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.core.eventbus.MessageConsumer;

import static com.ecosense.server.utils.Utils.buildErrorMessage;
import static com.ecosense.server.utils.Utils.buildSuccessMessage;

public class SensorVerticle extends AbstractVerticle implements Logger {
  public static final String SENSOR_SERVICE_ADDRESS = "sensor-database-service";

  private SensorRepository sensorRepository;
  private MessageConsumer<JsonObject> consumer;

  @Override
  public void init(Vertx vertx, Context context) {
    super.init(vertx, context);
    io.vertx.reactivex.core.Vertx rxVertx = io.vertx.reactivex.core.Vertx.vertx();
    this.sensorRepository = new MongoSensorRepository(rxVertx);
  }

  @Override
  public void start(Future<Void> startFuture) throws Exception {
    consumer = vertx.eventBus().consumer(SENSOR_SERVICE_ADDRESS, message -> {
      String action = message.headers().get("action");
      switch (action) {
        case "save":
          sensorRepository
            .save(message.body())
            .subscribe(response -> buildSuccessMessage(message, response), error -> buildErrorMessage(message, 500));
          break;
        case "find":
          sensorRepository
            .find(message.body())
            .subscribe(message::reply, error -> buildErrorMessage(message, 500));
          break;
        case "getIps":
          sensorRepository
            .getIps()
            .subscribe(response -> buildSuccessMessage(message, response), error -> buildErrorMessage(message, 500));
          break;
        case "updateTag":
          sensorRepository
            .updateTag(message.body())
            .subscribe(response -> buildSuccessMessage(message, response), error -> buildErrorMessage(message, 500));

          break;
        case "addMeasurements":
          sensorRepository
            .addMeasurements(message.body())
            .subscribe(response -> buildSuccessMessage(message, response), error -> buildErrorMessage(message, 500));
          break;
        default:
          message.fail(400, "Unknown action.");
      }
    });

    consumer.rxCompletionHandler()
      .subscribe(() -> {
        logger().info("Database Verticle deployed");
        startFuture.complete();
      }, error -> startFuture.fail("Failed to deploy Database Verticle"));
  }
}
