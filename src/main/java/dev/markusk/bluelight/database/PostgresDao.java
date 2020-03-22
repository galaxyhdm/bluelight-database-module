package dev.markusk.bluelight.database;

import dev.markusk.bluelight.api.builder.ArticleBuilder;
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
import java.sql.*;

public class PostgresDao implements SqlDao {

  private static final Logger LOGGER = LogManager.getLogger();

  private static final String SELECT_ARTICLE = "SELECT articles.* FROM articles WHERE article_id = ?;";
  private static final String HAS_ARTICLE = "SELECT articles.article_id FROM articles WHERE article_id = ?";
  private static final String SELECT_LOCATION = "SELECT locations.* FROM locations WHERE uuid = ?;";
  private static final String HAS_LOCATION = "SELECT locations.uuid FROM locations WHERE uuid = ?";
  private static final String HAS_LOCATION_NAME = "SELECT locations.uuid FROM locations WHERE location = ?";
  private static final String SELECT_TOPIC = "SELECT topics.* FROM topics WHERE uuid = ?;";
  private static final String HAS_TOPIC = "SELECT topics.uuid FROM topics WHERE uuid = ?";
  private static final String HAS_TOPIC_NAME = "SELECT topics.uuid FROM topics WHERE topic = ?";

  private static final String INSERT_ARTICLE =
      "INSERT INTO articles(article_id, title, url, release_time, fetch_time, file_hash, article_content) VALUES (?,?,?,?,?,?,?)";
  private static final String INSERT_LOCATION =
      "INSERT INTO locations(location, latitude, longitude, indexed) VALUES (?,?,?,false);";
  private static final String INSERT_LOCATION_WITH_NULL =
      "INSERT INTO locations(location, indexed) VALUES (?,false);";
  private static final String INSERT_TOPIC = "INSERT INTO topics(topic) VALUES (?)";
  private static final String INSERT_LOCATION_LINK =
      "INSERT INTO article_location(article_id, location_uuid) VALUES (?, (SELECT locations.uuid FROM locations WHERE locations.location=?))";
  private static final String INSERT_TOPIC_LINK =
      "INSERT INTO article_topic(article_id, topic_uuid) VALUES (?, (SELECT topics.uuid FROM topics WHERE topic=?))";

  private static final String UPDATE_ARTICLE_CONTENT = "UPDATE articles SET article_content=? WHERE article_id=?";

  private Connection connection;
  private PostgresDataManager dataSource;

  public PostgresDao(final PostgresDataManager dataSource) throws SQLException {
    this.dataSource = dataSource;
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

  @Override
  public void addArticle(final Article article) throws SQLException {
    try (final PreparedStatement preparedStatement = this.connection.prepareStatement(INSERT_ARTICLE)) {
      preparedStatement.setString(1, article.getId());
      preparedStatement.setString(2, article.getTitle());
      preparedStatement.setString(3, article.getUrl());
      preparedStatement.setTimestamp(4, new Timestamp(article.getReleaseTime().getTime()));
      preparedStatement.setTimestamp(5, new Timestamp(article.getFetchTime().getTime()));
      preparedStatement.setString(6, article.getFileIdentification());
      preparedStatement.setString(7, article.getContent());
      preparedStatement.execute();
    }
  }

  @Override
  public Article getArticle(final String id) throws SQLException {
    try (final PreparedStatement preparedStatement = this.connection.prepareStatement(SELECT_ARTICLE)) {
      preparedStatement.setString(1, id);
      final ResultSet resultSet = preparedStatement.executeQuery();
      if (!resultSet.next()) return null;
      final Article article = new ArticleBuilder()
          .id(resultSet.getString("article_id"))
          .title(resultSet.getString("title"))
          .url(resultSet.getString("url"))
          .releaseTime(resultSet.getTimestamp("release_time"))
          .fetchTime(resultSet.getTimestamp("fetch_time"))
          .fileIdentification(resultSet.getString("file_hash"))
          .content(resultSet.getString("article_content"))
          .createArticle();
      return article;
    }
  }

  @Override
  public void updateArticle(final Article article) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateArticleContent(final Article article) throws SQLException {
    try (final PreparedStatement preparedStatement = this.connection.prepareStatement(UPDATE_ARTICLE_CONTENT)) {
      preparedStatement.setString(1, article.getContent());
      preparedStatement.setString(2, article.getId());
      preparedStatement.execute();
    }
  }

  @Override
  public boolean hasArticle(final String id) {
    return has(HAS_ARTICLE, id);
  }

  @Override
  public void updateLocationLinks(final Article article) throws SQLException { //Only setting is currently available
    if (article.getLocationTags() == null || article.getLocationTags().isEmpty())
      throw new NullPointerException("location tags must be set!");
    for (final Location locationTag : article.getLocationTags()) {
      if (!this.hasLocationByName(locationTag.getLocationName())) {
        this.addLocation(locationTag);
      }
      this.addLocationLink(article.getId(), locationTag.getLocationName());
    }
  }

  private void addLocationLink(final String articleId, final String locationName) throws SQLException {
    try (final PreparedStatement preparedStatement = this.connection.prepareStatement(INSERT_LOCATION_LINK)) {
      preparedStatement.setString(1, articleId);
      preparedStatement.setString(2, locationName);
      preparedStatement.execute();
    }
  }

  @Override
  public void updateTopicLinks(final Article article) throws SQLException {
    if (article.getTopicTags() == null || article.getTopicTags().isEmpty())
      throw new NullPointerException("topic tags must be set!");
    for (final Topic topic : article.getTopicTags()) {
      if (!this.hasTopicByName(topic.getTopicName())) {
        this.addTopic(topic);
      }
      this.addTopicLink(article.getId(), topic.getTopicName());
    }
  }

  private void addTopicLink(final String articleId, final String topicName) throws SQLException {
    try (final PreparedStatement preparedStatement = this.connection.prepareStatement(INSERT_TOPIC_LINK)) {
      preparedStatement.setString(1, articleId);
      preparedStatement.setString(2, topicName);
      preparedStatement.execute();
    }
  }

  @Override
  public void addLocation(final Location location) throws SQLException {
    if (location.getLatitude() == null || location.getLongitude() == null) {
      try (final PreparedStatement preparedStatement = this.connection.prepareStatement(INSERT_LOCATION_WITH_NULL)) {
        preparedStatement.setString(1, location.getLocationName());
        preparedStatement.execute();
      }
    } else {
      try (final PreparedStatement preparedStatement = this.connection.prepareStatement(INSERT_LOCATION)) {
        preparedStatement.setString(1, location.getLocationName());
        preparedStatement.setDouble(2, location.getLatitude());
        preparedStatement.setDouble(3, location.getLongitude());
        preparedStatement.execute();
      }
    }
  }

  @Override
  public Location getLocation(final String id) throws SQLException {
    try (final PreparedStatement preparedStatement = this.connection.prepareStatement(SELECT_LOCATION)) {
      preparedStatement.setString(1, id);
      final ResultSet resultSet = preparedStatement.executeQuery();
      if (!resultSet.next()) return null;
      final Location location = new Location();

      location.setId(resultSet.getString("uuid"));
      location.setLocationName(resultSet.getString("location"));
      location.setLatitude(resultSet.getDouble("latitude"));
      location.setLongitude(resultSet.getDouble("longitude"));
      location.setIndexed(resultSet.getBoolean("indexed"));
      return location;
    }
  }

  @Override
  public boolean hasLocation(final String id) {
    return has(HAS_LOCATION, id);
  }

  @Override
  public boolean hasLocationByName(final String locationName) throws SQLException {
    return has(HAS_LOCATION_NAME, locationName);
  }

  @Override
  public void addTopic(final Topic topic) throws SQLException {
    try (final PreparedStatement preparedStatement = this.connection.prepareStatement(INSERT_TOPIC)) {
      preparedStatement.setString(1, topic.getTopicName());
      preparedStatement.execute();
    }
  }

  @Override
  public Topic getTopic(final String id) throws SQLException {
    try (final PreparedStatement preparedStatement = this.connection.prepareStatement(SELECT_TOPIC)) {
      preparedStatement.setString(1, id);
      final ResultSet resultSet = preparedStatement.executeQuery();
      if (!resultSet.next()) return null;
      final Topic topic = new Topic();

      topic.setId(resultSet.getString("uuid"));
      topic.setTopicName(resultSet.getString("topic"));
      return topic;
    }
  }

  @Override
  public boolean hasTopic(final String id) {
    return has(HAS_TOPIC, id);
  }

  @Override
  public boolean hasTopicByName(final String topicName) throws SQLException {
    return has(HAS_TOPIC_NAME, topicName);
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

  private boolean has(final String query, final String firstIdentifier) {
    try (PreparedStatement preparedStatement = this.connection.prepareStatement(query)) {
      preparedStatement.setString(1, firstIdentifier);
      try (ResultSet resultSet = preparedStatement.executeQuery()) {
        return resultSet.next();
      }
    } catch (SQLException e) {
      LOGGER.error("Error while executing query", e);
    }
    return false;
  }

  private boolean hasTable(final String tableName) throws SQLException {
    return this.connection.getMetaData().getTables(null, null, tableName, null).next();
  }

  @Override
  public void close() throws Exception {
    this.connection.close();
  }
}
