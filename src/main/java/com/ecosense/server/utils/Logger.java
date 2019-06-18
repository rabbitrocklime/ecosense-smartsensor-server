package com.ecosense.server.utils;

import io.vertx.core.logging.LoggerFactory;

public interface Logger {
  default io.vertx.core.logging.Logger logger() {
    return LoggerFactory.getLogger(this.getClass());
  }
}
