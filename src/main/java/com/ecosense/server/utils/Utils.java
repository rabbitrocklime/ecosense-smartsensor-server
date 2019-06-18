package com.ecosense.server.utils;

import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.eventbus.Message;

import java.util.List;

public class Utils {
  public static DeliveryOptions action(String action) {
    return new DeliveryOptions().addHeader("action", action);
  }

  public static void buildSuccessMessage(Message<JsonObject> message, JsonObject jsonObject) {
    message.reply(new JsonObject()
      .put("success", true)
      .put("data", jsonObject));
  }

  public static void buildSuccessMessage(Message<JsonObject> message, List<JsonObject> jsonList) {
    JsonArray ips = jsonList.stream()
      .map(jsonObject -> new JsonObject()
        .put("ip", jsonObject.getString("ip"))
        .put("tag", jsonObject.getString("tag", "")))
      .collect(JsonArray::new, JsonArray::add, JsonArray::addAll);

    message.reply(new JsonObject()
      .put("success", true)
      .put("data", ips));
  }

  public static void buildSuccessMessage(Message<JsonObject> message) {
    message.reply(new JsonObject()
      .put("success", true)
    );
  }

  public static void buildSuccessMessage(Message<JsonObject> message, JsonArray jsonArray) {
    message.reply(new JsonObject()
      .put("success", true)
      .put("data", jsonArray));
  }

  public static void buildSuccessMessage(Message<JsonObject> message, String string) {
    message.reply(new JsonObject()
      .put("success", true)
      .put("data", string));
  }

  public static void buildErrorMessage(Message<JsonObject> message, int status) {
    message.reply(new JsonObject()
      .put("success", false)
      .put("failureCode", status));
  }

}
