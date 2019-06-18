package com.ecosense.server;

import com.ecosense.server.api.server.ApiServerVerticle;
import com.ecosense.server.database.SensorVerticle;
import com.ecosense.server.node.client.NodeClientVerticle;
import com.ecosense.server.node.server.NodeServerVerticle;
import com.ecosense.server.utils.Logger;
import io.reactivex.Completable;
import io.vertx.core.Future;
import io.vertx.reactivex.core.AbstractVerticle;

public class MainVerticle extends AbstractVerticle implements Logger {

  @Override
  public void start(Future<Void> future) {
    Completable.concatArray(
      vertx.rxDeployVerticle(SensorVerticle.class.getName()).ignoreElement(),
      vertx.rxDeployVerticle(NodeServerVerticle.class.getName()).ignoreElement(),
      vertx.rxDeployVerticle(NodeClientVerticle.class.getName()).ignoreElement(),
      vertx.rxDeployVerticle(ApiServerVerticle.class.getName()).ignoreElement())
      .subscribe(() -> {
        logger().info("Verticles deployed");
        future.complete();
      }, error ->
        future.fail("Failed to deploy verticles"));
  }
}
