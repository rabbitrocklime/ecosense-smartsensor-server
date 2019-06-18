package com.ecosense.server.database;

import io.reactivex.Maybe;
import io.reactivex.Single;
import io.vertx.core.json.JsonObject;

import java.util.List;

public interface SensorRepository {
  Maybe<String> save(JsonObject sensor);
  Single<List<JsonObject>> find(JsonObject query);
  Single<List<JsonObject>> getIps();
  Maybe<JsonObject> updateTag(JsonObject updateTag);
  Maybe<JsonObject> addMeasurements(JsonObject measurements);

  }
