package dev.markusk.bluelight.database;

public class Environment {

  public static final String SQL_HOST = String.valueOf(System.getenv("SQL_HOST"));
  public static final int SQL_PORT = Integer.parseInt(System.getenv("SQL_PORT"));
  public static final String SQL_DATABASE = String.valueOf(System.getenv("SQL_DATABASE"));
  public static final String SQL_USER = String.valueOf(System.getenv("SQL_USER"));
  public static final String SQL_PASSWORD = String.valueOf(System.getenv("SQL_PASSWORD"));

}
