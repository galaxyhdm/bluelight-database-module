package dev.markusk.bluelight.database;

import ch.qos.logback.classic.Level;
import dev.markusk.bluelight.api.builder.ArticleBuilder;
import dev.markusk.bluelight.api.objects.Article;
import dev.markusk.bluelight.api.objects.Location;
import dev.markusk.bluelight.api.objects.Topic;
import liquibase.Liquibase;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.apache.logging.log4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class PostgresDao implements SqlDao {

  private static final String SELECT_ARTICLES = "SELECT articles.* FROM articles;";
  private static final String SELECT_ARTICLE = "SELECT articles.* FROM articles WHERE article_id = ?;";
  private static final String HAS_ARTICLE = "SELECT articles.article_id FROM articles WHERE article_id = ?";
  private static final String SELECT_LOCATION = "SELECT locations.* FROM locations WHERE uuid = ?;";
  private static final String HAS_LOCATION = "SELECT locations.uuid FROM locations WHERE uuid = ?";
  private static final String HAS_LOCATION_NAME = "SELECT locations.uuid FROM locations WHERE location = ?";
  private static final String SELECT_TOPIC = "SELECT topics.* FROM topics WHERE uuid = ?;";
  private static final String HAS_TOPIC = "SELECT topics.uuid FROM topics WHERE uuid = ?";
  private static final String HAS_TOPIC_NAME = "SELECT topics.uuid FROM topics WHERE topic = ?";
  private static final String SELECT_LOCATIONS_FROM_ARTICLE =
      "SELECT locations.* FROM locations INNER JOIN article_location al on locations.uuid = al.location_uuid WHERE al.article_id = ?";
  private static final String SELECT_TOPICS_FROM_ARTICLE =
      "SELECT topics.* FROM topics INNER JOIN article_topic a on topics.uuid = a.topic_uuid WHERE a.article_id = ?";

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

  private final Logger logger;
  private final Connection connection;
  private final PostgresDataManager dataSource;

  public PostgresDao(final PostgresDataManager dataSource) throws SQLException {
    this.dataSource = dataSource;
    this.connection = dataSource.getDataSource().getConnection();
    this.logger = dataSource.getLogger();
  }

  @Override
  public void initializeTables() {
    this.setupLiquibaseLogger();
    try (Liquibase liquibase = new Liquibase("schema/db.changelog-master.xml", new ClassLoaderResourceAccessor(),
        DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(this.connection)))) {
      liquibase.update("");
    } catch (Exception e) {
      this.logger.error("Error initializeTables", e);
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
      return this.getArticleByResult(resultSet);
    }
  }

  @Override
  public List<Article> getArticles() throws SQLException {
    final List<Article> articles = new ArrayList<>();
    try (final PreparedStatement preparedStatement = this.connection.prepareStatement(SELECT_ARTICLES)) {
      final ResultSet resultSet = preparedStatement.executeQuery();
      while (resultSet.next()) {
        articles.add(getArticleByResult(resultSet));
      }
    }
    return articles;
  }

  private Article getArticleByResult(final ResultSet resultSet) throws SQLException {
    return new ArticleBuilder()
        .id(resultSet.getString("article_id"))
        .title(resultSet.getString("title"))
        .url(resultSet.getString("url"))
        .releaseTime(resultSet.getTimestamp("release_time"))
        .fetchTime(resultSet.getTimestamp("fetch_time"))
        .fileIdentification(resultSet.getString("file_hash"))
        .content(resultSet.getString("article_content"))
        .createArticle();
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
  public void updateLocationLinks(final Article article)
      throws SQLException { //todo Only set is currently available, update to remove usw.
    if (article.getLocationTags() == null || article.getLocationTags().isEmpty())
      throw new NullPointerException("location tags must be set!");
    for (final Location locationTag : article.getLocationTags()) {
      if (!this.hasLocationByName(locationTag.getLocationName())) {
        this.addLocation(locationTag);
      }
      this.addLocationLink(article.getId(), locationTag.getLocationName());
    }
  }

  private void addLocationLink(final String articleId, final String locationName)
      throws SQLException { // check for existing
    try (final PreparedStatement preparedStatement = this.connection.prepareStatement(INSERT_LOCATION_LINK)) {
      preparedStatement.setString(1, articleId);
      preparedStatement.setString(2, locationName);
      preparedStatement.execute();
    }
  }

  @Override
  public Set<Location> getLocations(final String articleId) throws SQLException {
    final Set<Location> locations = new HashSet<>();
    try (final PreparedStatement preparedStatement = this.connection.prepareStatement(SELECT_LOCATIONS_FROM_ARTICLE)) {
      preparedStatement.setString(1, articleId);
      final ResultSet resultSet = preparedStatement.executeQuery();
      while (resultSet.next()) {
        locations.add(getLocationFromResult(resultSet));
      }
    }
    return locations;
  }

  @Override
  public void updateTopicLinks(final Article article)
      throws SQLException { //todo Only set is currently available, update to remove usw.
    if (article.getTopicTags() == null || article.getTopicTags().isEmpty())
      throw new NullPointerException("topic tags must be set!");
    for (final Topic topic : article.getTopicTags()) {
      if (!this.hasTopicByName(topic.getTopicName())) {
        this.addTopic(topic);
      }
      this.addTopicLink(article.getId(), topic.getTopicName());
    }
  }

  private void addTopicLink(final String articleId, final String topicName) throws SQLException {  // check for existing
    try (final PreparedStatement preparedStatement = this.connection.prepareStatement(INSERT_TOPIC_LINK)) {
      preparedStatement.setString(1, articleId);
      preparedStatement.setString(2, topicName);
      preparedStatement.execute();
    }
  }

  @Override
  public Set<Topic> getTopics(final String articleId) throws SQLException {
    final Set<Topic> topics = new HashSet<>();
    try (final PreparedStatement preparedStatement = this.connection.prepareStatement(SELECT_TOPICS_FROM_ARTICLE)) {
      preparedStatement.setString(1, articleId);
      final ResultSet resultSet = preparedStatement.executeQuery();
      while (resultSet.next()) {
        topics.add(getTopicFromResult(resultSet));
      }
    }
    return topics;
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
      return getLocationFromResult(resultSet);
    }
  }

  @Override
  public boolean hasLocation(final String id) {
    return has(HAS_LOCATION, id);
  }

  @Override
  public boolean hasLocationByName(final String locationName) {
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
      return getTopicFromResult(resultSet);
    }
  }

  @Override
  public boolean hasTopic(final String id) {
    return has(HAS_TOPIC, id);
  }

  @Override
  public boolean hasTopicByName(final String topicName) {
    return has(HAS_TOPIC_NAME, topicName);
  }

  private Location getLocationFromResult(final ResultSet resultSet) throws SQLException {
    final Location location = new Location();
    location.setId(resultSet.getString("uuid"));
    location.setLocationName(resultSet.getString("location"));
    location.setLatitude(resultSet.getDouble("latitude"));
    location.setLongitude(resultSet.getDouble("longitude"));
    location.setIndexed(resultSet.getBoolean("indexed"));
    return location;
  }

  private Topic getTopicFromResult(final ResultSet resultSet) throws SQLException {
    final Topic topic = new Topic();
    topic.setId(resultSet.getString("uuid"));
    topic.setTopicName(resultSet.getString("topic"));
    return topic;
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
            logger.debug("STREAM EXECUTION > add Batch: " + queryLine.replaceAll(" +", " "));
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
      logger.error("Error while executing query", e);
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

  private void setupLiquibaseLogger() {
    final org.slf4j.Logger liquibaseLogger = LoggerFactory.getLogger("liquibase");
    if (!(liquibaseLogger instanceof ch.qos.logback.classic.Logger)) return;
    ((ch.qos.logback.classic.Logger) liquibaseLogger).setLevel(Environment.DEBUG ? Level.INFO : Level.OFF);
  }

}
