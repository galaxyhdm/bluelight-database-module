package dev.markusk.bluelight.database;

import java.sql.SQLException;

public interface SqlDao extends AutoCloseable {

  void initializeTables() throws SQLException;

}
