package dev.markusk.bluelight.database;

public class Environment {

  public static final boolean DEBUG = System.getenv("DEBUG") != null && Boolean.parseBoolean(System.getenv("DEBUG"));

}
