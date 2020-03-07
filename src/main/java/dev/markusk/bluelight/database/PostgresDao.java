package dev.markusk.bluelight.database;

import dev.markusk.bluelight.api.objects.Article;
import dev.markusk.bluelight.api.objects.Location;
import dev.markusk.bluelight.api.objects.Topic;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

public class PostgresDao implements SqlDao {

  private static final Logger LOGGER = LogManager.getLogger();

  private static final String INSERT_ARTICLE =
      "INSERT INTO articles(article_id, title, url, release_time, fetch_time, file_hash, article_content) VALUES (?,?,?,?,?,?,?)";

  private Connection connection;

  public PostgresDao(final PostgresDataManager dataSource) throws SQLException {
    this.connection = dataSource.getDataSource().getConnection();
  }

  @Override
  public void initializeTables() throws SQLException {
    if (this.hasTable("articles")) return;
    final String database = this.connection.getMetaData().getDatabaseProductName().toLowerCase();
    try (final InputStream stream = SqlDao.class.getResourceAsStream(String.format("/schema/%s.sql", database))) {
      if (stream == null) {
        LOGGER.error("Initial schema for " + database + " not available!");
        return;
      }
      try (final BufferedReader bufferedReader = new BufferedReader(
          new InputStreamReader(stream, StandardCharsets.UTF_8))) {
        LOGGER.info("Creating schema: " + database);
        this.executeStream(bufferedReader);
        LOGGER.info("Created schema: " + database);
      }
    } catch (IOException e) {
      LOGGER.error("Error initializeTables", e);
    }
  }

  public void addArticle(final Article article) throws SQLException {
    try (final PreparedStatement preparedStatement = this.connection.prepareStatement(INSERT_ARTICLE)) {
      preparedStatement.setString(1, article.getId());
      preparedStatement.setString(2, article.getTitle());

    }
  }

  public void updateArticle(final Article article) {

  }

  public void updateArticleContent(final Article article) {

  }

  public boolean hasArticle(final String id) {
    return false;
  }

  public void updateLocationLinks(final Article article) {

  }

  public void updateTopicLinks(final Article article) {

  }

  public void addLocation(final Location location) {

  }

  public Location getLocation(final String id) {
    return null;
  }

  public boolean hasLocation(final String id) {
    return false;
  }

  public void addTopic(final Topic topic) {

  }

  public Topic getTopic(final String id) {
    return null;
  }

  public boolean hasTopic(final String id) {
    return false;
  }

  private void executeStream(final BufferedReader bufferedReader) throws SQLException, IOException {
    try (Statement statement = this.connection.createStatement()) {
      StringBuilder stringBuilder = new StringBuilder();
      String line;
      while ((line = bufferedReader.readLine()) != null) {
        if (line.startsWith("--")) continue;
        stringBuilder.append(line);
        if (line.endsWith(";")) {
          stringBuilder.deleteCharAt(stringBuilder.length() - 1);
          String queryLine = stringBuilder.toString().trim();
          stringBuilder = new StringBuilder();
          if (!queryLine.isEmpty()) {
            statement.addBatch(queryLine);
            LOGGER.debug("STREAM EXECUTION > add Batch: " + queryLine.replaceAll(" +", " "));
          }
        }
      }
      statement.executeBatch();
    }
  }

  private boolean hasTable(final String tableName) throws SQLException {
    return this.connection.getMetaData().getTables(null, null, tableName, null).next();
  }

  @Override
  public void close() throws Exception {
    this.connection.close();
  }
}
