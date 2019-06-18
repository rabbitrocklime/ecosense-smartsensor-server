package com.ecosense.server.node.client;

import com.ecosense.server.utils.Logger;
import io.netty.handler.codec.http.QueryStringEncoder;
import io.reactivex.Observable;
import io.reactivex.Single;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.core.buffer.Buffer;
import io.vertx.reactivex.core.eventbus.Message;
import io.vertx.reactivex.ext.web.client.HttpResponse;
import io.vertx.reactivex.ext.web.client.WebClient;

import java.util.List;
import java.util.stream.Collectors;

import static com.ecosense.server.database.SensorVerticle.SENSOR_SERVICE_ADDRESS;
import static com.ecosense.server.utils.Utils.action;
import static com.ecosense.server.utils.Utils.buildErrorMessage;

public class NodeClientVerticle extends AbstractVerticle implements Logger {
  public static final String NODE_CLIENT_ADDRESS = "node-client-service";
  private int interval = 10000;
  private WebClient client;

  @Override
  public void start() {
    vertx.eventBus().consumer(NODE_CLIENT_ADDRESS, this::handleMessages);
    client = WebClient.create(vertx);

    vertx.setPeriodic(interval, fireMeasurementsRequests());
  }

  private void handleMessages(Message<JsonObject> message) {
    String action = message.headers().get("action");

    switch (action) {
      case "setInterval":
        handleSetInterval(message);
        break;
      case "setCollectingInterval":
        handleCollectingInterval(message);
        break;
      case "getTemperature":
        handleGetMeasurement(message, "getTemperature");
        break;
      case "getHumidity":
        handleGetMeasurement(message, "getHumidity");
        break;
      case "getPressure":
        handleGetMeasurement(message, "getPressure");
        break;
      case "setWarningValues":
        handleSetWarningValues(message);
      default:
        message.fail(500, "Path not known");
        break;
    }
  }

  private void handleSetWarningValues(Message<JsonObject> message) {
    String ip = (String) message.body().remove("ip");

    QueryStringEncoder queryString = new QueryStringEncoder("/warning");
    message.body().stream()
      .forEach(entry -> queryString.addParam(entry.getKey(), entry.getValue().toString()));

    client
      .get(ip, queryString.toString())
      .rxSend()
      .subscribe(message::reply, error -> buildErrorMessage(message, 500));
  }

  private void handleGetMeasurement(Message<JsonObject> message, String type) {
    String ip = message.body().getString("ip");
    String request = "/" + type;
    client
      .get(ip, request)
      .rxSend()
      .subscribe(message::reply, error -> buildErrorMessage(message, 500));
  }

  private void handleSetInterval(Message<JsonObject> message) {
    interval = message.body().getInteger("interval", 10000);
    message
      .rxReply(new JsonObject().put("success", true))
      .subscribe();
  }

  private void handleCollectingInterval(Message<JsonObject> message) {
    String ip = message.body().getString("ip");
    String interval = String.valueOf(message.body().getInteger("interval"));

    String request = "/interval?time=" + interval;

    client
      .get(ip, request)
      .rxSend()
      .subscribe(message::reply, error -> buildErrorMessage(message, 500));
  }

  private Handler<Long> fireMeasurementsRequests() {
    return t -> {
      Single<List<String>> getIps =
        vertx.eventBus().<JsonObject>rxSend(SENSOR_SERVICE_ADDRESS, new JsonObject(), action("getIps"))
          .map(this::parseMessageToStringList);

      getIps
        .flattenAsObservable(a -> a)
        .flatMap(this::getMeasurements)
        .filter(bufferHttpResponse -> bufferHttpResponse.statusCode() == 200)
        .map(HttpResponse::bodyAsJsonObject)
        .flatMapSingle(jsonObject -> vertx.eventBus().<JsonObject>rxSend(SENSOR_SERVICE_ADDRESS, jsonObject, action("addMeasurements")))
        .subscribe(success -> logger().info(success.body().encodePrettily()), error -> logger().error(error.getMessage()));
    };
  }

  private Observable<HttpResponse<Buffer>> getMeasurements(String ipAddress) {
    return client
      .get(ipAddress, "/flush")
      .putHeader("Content-type", "application/json")
      .rxSend()
      .toObservable();
  }

  private List<String> parseMessageToStringList(Message<JsonObject> jsonObjectMessage) {
    return jsonObjectMessage.body().getJsonArray("data")
      .stream()
      .map(JsonObject.class::cast)
      .map(jsonObject -> jsonObject.getString("ip"))
      .collect(Collectors.toList());
  }
}
