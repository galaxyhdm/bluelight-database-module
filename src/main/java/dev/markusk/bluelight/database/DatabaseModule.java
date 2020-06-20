package dev.markusk.bluelight.database;

import dev.markusk.bluelight.api.modules.Module;

public class DatabaseModule extends Module {

  @Override
  public void enable() {
    this.getFetcher().getDataRegistry().register("postgres", PostgresDataManager.class);
  }

  @Override
  public void disable() {

  }
}
