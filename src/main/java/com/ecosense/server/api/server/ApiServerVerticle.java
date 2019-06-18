package com.ecosense.server.api.server;

import com.ecosense.server.utils.Logger;
import io.reactivex.functions.Consumer;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.core.eventbus.Message;
import io.vertx.reactivex.core.http.HttpServer;
import io.vertx.reactivex.core.http.HttpServerResponse;
import io.vertx.reactivex.ext.web.Router;
import io.vertx.reactivex.ext.web.RoutingContext;

import java.util.List;
import java.util.Map;

import static com.ecosense.server.database.SensorVerticle.SENSOR_SERVICE_ADDRESS;
import static com.ecosense.server.node.client.NodeClientVerticle.NODE_CLIENT_ADDRESS;
import static com.ecosense.server.utils.Utils.action;

public class ApiServerVerticle extends AbstractVerticle implements Logger {
  @Override
  public void start(Future<Void> future) {
    HttpServer httpServer = vertx.createHttpServer();

    httpServer
      .requestHandler(getRouter())
      .rxListen(9000)
      .ignoreElement()
      .subscribe(() -> {
        logger().info("Api Server Verticle deployed");
        future.complete();
      }, error -> future.fail("Failed to deploy Api Server Verticle"));
  }

  private Router getRouter() {
    Router router = Router.router(vertx);
    router.get("/ip/:ip/tag/:tag").handler(this::handleSetTag);
    router.get("/ips").handler(this::handleGetIps);
    router.get("/getdata/ip/:ip").handler(this::handleGetDataIp);
    router.get("/getdata/tag/:tag").handler(this::handleGetDataTag);
    router.get("/setflushinterval/:interval").handler(this::handleSetFlushInterval);
    router.get("/ip/:ip/interval/:interval").handler(this::handleSetCollectingIntervalOnSensor);
    router.get("/ip/:ip/gettemperature").handler(routingContext -> handleGetTemperature(routingContext, "getTemperature"));
    router.get("/ip/:ip/gethumidity").handler(routingContext -> handleGetTemperature(routingContext, "getHumidity"));
    router.get("/ip/:ip/getpressure").handler(routingContext -> handleGetTemperature(routingContext, "getPressure"));
    router.get("/warning/ip/:ip").handler(this::handleSetWarning);
    return router;
  }

  private void handleSetWarning(RoutingContext routingContext) {
    String ip = routingContext.pathParam("ip");
    JsonObject query = new JsonObject()
      .put("ip", ip);

    List<Map.Entry<String, String>> entries = routingContext.queryParams().entries();
    for (Map.Entry entry : entries) {
      query.put(entry.getKey().toString(), entry.getValue().toString());
    }

    vertx.eventBus().<JsonObject>rxSend(NODE_CLIENT_ADDRESS, query, action("setWarningValues"))
      .subscribe(buildResponseMessage(routingContext), Throwable::printStackTrace);
  }

  private void handleGetTemperature(RoutingContext routingContext, String action) {
    String ip = routingContext.pathParam("ip");

    JsonObject query = new JsonObject()
      .put("ip", ip);

    vertx.eventBus().<JsonObject>rxSend(NODE_CLIENT_ADDRESS, query, action(action))
      .subscribe(buildResponseMessage(routingContext), Throwable::printStackTrace);
  }


  private void handleSetCollectingIntervalOnSensor(RoutingContext routingContext) {
    int interval = Integer.parseInt(routingContext.pathParam("interval"));
    String ip = routingContext.pathParam("ip");
    JsonObject request = new JsonObject()
      .put("ip", ip)
      .put("interval", interval);

    vertx.eventBus().<JsonObject>rxSend(NODE_CLIENT_ADDRESS, request, action("setCollectingInterval"))
      .subscribe(buildEmptyResponseMessage(routingContext), Throwable::printStackTrace);
  }

  private void handleSetFlushInterval(RoutingContext routingContext) {
    int interval = Integer.parseInt(routingContext.pathParam("interval"));
    vertx.eventBus().<JsonObject>rxSend(NODE_CLIENT_ADDRESS, new JsonObject().put("interval", interval))
      .subscribe(buildEmptyResponseMessage(routingContext), Throwable::printStackTrace);
  }

  private void handleGetDataTag(RoutingContext routingContext) {
    String tag = routingContext.pathParam("tag");
    JsonObject query = new JsonObject()
      .put("tag", tag);

    vertx.eventBus().<JsonObject>rxSend(SENSOR_SERVICE_ADDRESS, query, action("find"))
      .subscribe(buildResponseMessage(routingContext), Throwable::printStackTrace);
  }

  private void handleGetDataIp(RoutingContext routingContext) {
    String ip = routingContext.pathParam("ip");
    JsonObject query = new JsonObject()
      .put("ip", ip);

    vertx.eventBus().<JsonObject>rxSend(SENSOR_SERVICE_ADDRESS, query, action("find"))
      .subscribe(buildResponseMessage(routingContext), Throwable::printStackTrace);
  }

  private void handleGetIps(RoutingContext routingContext) {
    vertx.eventBus().<JsonObject>rxSend(SENSOR_SERVICE_ADDRESS, new JsonObject(), action("getIps"))
      .subscribe(buildResponseMessage(routingContext), Throwable::printStackTrace);

  }

  private void handleSetTag(RoutingContext routingContext) {
    String tag = routingContext.pathParam("tag");
    String ip = routingContext.pathParam("ip");

    JsonObject updateTag = new JsonObject()
      .put("ip", ip)
      .put("tag", tag);

    vertx.eventBus().<JsonObject>rxSend(SENSOR_SERVICE_ADDRESS, updateTag, action("updateTag"))
      .subscribe(getMessageTag(routingContext), Throwable::printStackTrace);
  }

  private Consumer<Message<JsonObject>> getMessageTag(RoutingContext routingContext) {
    return message -> {
      HttpServerResponse response = routingContext.response();
      JsonObject body = message.body();
      if (body.getBoolean("success")) {
        response
          .putHeader("Content-type", "application/json")
          .setStatusCode(200)
          .end(new JsonObject().put("tagUpdated", true).toString());
      } else {
        response
          .putHeader("Content-type", "text/plain")
          .setStatusCode(body.getInteger("failureCode", 500))
          .end();
      }
    };
  }

  private Consumer<Message<JsonObject>> buildResponseMessage(RoutingContext routingContext) {
    return message -> {
      HttpServerResponse response = routingContext.response();
      JsonObject body = message.body();
      if (body.getBoolean("success")) {
        response
          .putHeader("Content-type", "application/json")
          .setStatusCode(200)
          .end(body.getJsonArray("data").toString());
      } else {
        response
          .putHeader("Content-type", "text/plain")
          .setStatusCode(body.getInteger("failureCode", 500))
          .end();
      }
    };
  }

  private Consumer<Message<JsonObject>> buildEmptyResponseMessage(RoutingContext routingContext) {
    return message -> {
      HttpServerResponse response = routingContext.response();
      JsonObject body = message.body();
      if (body.getBoolean("success")) {
        response
          .setStatusCode(200)
          .end();
      } else {
        response
          .putHeader("Content-type", "text/plain")
          .setStatusCode(body.getInteger("failureCode", 500))
          .end();
      }
    };
  }

}
