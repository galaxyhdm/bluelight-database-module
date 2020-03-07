package dev.markusk.bluelight.database;

import com.google.common.collect.ImmutableMap;
import dev.markusk.bluelight.api.AbstractFetcher;
import dev.markusk.bluelight.api.data.AbstractDataManager;
import dev.markusk.bluelight.api.objects.Article;
import dev.markusk.bluelight.api.objects.Location;
import dev.markusk.bluelight.api.objects.Topic;
import dev.markusk.projectbluelight.util.ThrowingFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.postgresql.ds.PGConnectionPoolDataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

public class PostgresDataManager implements AbstractDataManager {

  private static final Logger LOGGER = LogManager.getLogger();

  private AbstractFetcher abstractFetcher;
  private PGConnectionPoolDataSource dataSource;

  private final Map<String, ThrowingFunction<PostgresDataManager, SqlDao, SQLException>> daoImplementations =
      ImmutableMap.of("postgresql", PostgresDao::new);
  private ThrowingFunction<PostgresDataManager, SqlDao, SQLException> daoFactory;

  private SqlDao getDao() throws SQLException {
    return this.daoFactory.apply(this);
  }

  @Override
  public boolean initialize(final AbstractFetcher abstractFetcher) {
    LOGGER.info("Connecting to postgres-database...");
    this.abstractFetcher = abstractFetcher;

    // TODO: 07.03.2020 add credentials to .env file
    this.dataSource = new PGConnectionPoolDataSource();
    this.dataSource.setServerNames(new String[]{Environment.SQL_HOST});
    this.dataSource.setPortNumbers(new int[]{Environment.SQL_PORT});
    this.dataSource.setDatabaseName(Environment.SQL_DATABASE);
    this.dataSource.setUser(Environment.SQL_USER);
    this.dataSource.setPassword(Environment.SQL_PASSWORD);

    try (final Connection connection = this.dataSource.getConnection()) {
      final String database = connection.getMetaData().getDatabaseProductName().toLowerCase();
      this.daoFactory = this.daoImplementations.get(database);
      if (this.daoFactory == null) {
        LOGGER.error(String.format("Database implementation %s is not supported!", database));
        return false;
      } else
        LOGGER.debug(String.format("Found supported database implementation: %s", database));
    } catch (Exception e) {
      LOGGER.error("Error while pooledConnection to database", e);
      return false;
    }

    try (SqlDao dao = this.getDao()) {
      dao.initializeTables();
    } catch (Exception e) {
      LOGGER.error("Could not connect to SQL database!", e);
      return false;
    }
    LOGGER.info(String.format("Connected to sql-database. (Database: %s)", "bminer"));
    return true;
  }

  @Override
  public void close() {
    this.dataSource = null;
    this.daoFactory = null;
    LOGGER.info("SqlDataSource closed!");
  }

  @Override
  public void addArticle(final Article article) {

  }

  @Override
  public Article getArticle(final String id, final boolean loadTags) {
    return null;
  }

  @Override
  public void updateArticle(final Article article) {

  }

  @Override
  public void updateArticleContent(final Article article) {

  }

  @Override
  public boolean hasArticle(final String id) {
    return false;
  }

  @Override
  public void updateLocationLinks(final Article article) {

  }

  @Override
  public void updateTopicLinks(final Article article) {

  }

  @Override
  public void addLocation(final Location location) {

  }

  @Override
  public Location getLocation(final String id) {
    return null;
  }

  @Override
  public boolean hasLocation(final String id) {
    return false;
  }

  @Override
  public void addTopic(final Topic topic) {

  }

  @Override
  public Topic getTopic(final String id) {
    return null;
  }

  @Override
  public boolean hasTopic(final String id) {
    return false;
  }

  public PGConnectionPoolDataSource getDataSource() {
    return dataSource;
  }

}
