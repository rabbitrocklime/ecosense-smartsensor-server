package com.ecosense.server.database;

import io.reactivex.Maybe;
import io.reactivex.Single;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.ext.mongo.MongoClient;

import java.sql.Timestamp;
import java.util.List;

public class MongoSensorRepository implements SensorRepository {
  private static final String SENSORS = "sensors";
  private final MongoClient mongoClient;

  public MongoSensorRepository(Vertx vertx) {
    this.mongoClient = MongoClient.createNonShared(
      vertx,
      new JsonObject()
        .put("db_name", "smartsensor")
    );
  }

  public Maybe<String> save(JsonObject registration) {
    String ip = registration.getString("ip");
    Timestamp timestamp = new Timestamp(System.currentTimeMillis());
    JsonObject newSensor = new JsonObject()
      .put("ip", ip)
      .put("startTime", timestamp.toString())
      .putNull("tag")
      .put("measurements", new JsonArray());
    return mongoClient.rxSave(SENSORS, newSensor);
  }

  public Single<List<JsonObject>> find(JsonObject query) {
    return mongoClient.rxFind(SENSORS, query);
  }

  @Override
  public Single<List<JsonObject>> getIps() {
    return mongoClient.rxFind(SENSORS, new JsonObject());
  }

  public Maybe<JsonObject> updateTag(JsonObject updateTag) {
    JsonObject query = new JsonObject().put("ip", updateTag.getString("ip"));
    JsonObject update = new JsonObject()
      .put("$set", new JsonObject()
        .put("tag", updateTag.getString("tag")));
    return mongoClient.rxFindOneAndUpdate(SENSORS, query, update);
  }

  public Maybe<JsonObject> addMeasurements(JsonObject measurements) {
    JsonObject query = new JsonObject().put("ip", measurements.getString("ip"));
    JsonObject update = new JsonObject()
      .put("$push", new JsonObject()
        .put("measurements", measurements.getJsonArray("measurements")));

    return mongoClient.rxFindOneAndUpdate(SENSORS, query, update);
  }

}
