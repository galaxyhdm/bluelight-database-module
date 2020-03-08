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
import java.util.Optional;

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
    try (final SqlDao dao = this.getDao()) {
      dao.addArticle(article);
    } catch (Exception e) {
      LOGGER.error("Error in addArticle", e);
    }
  }

  @Override
  public Optional<Article> getArticle(final String id, final boolean loadTags) { // TODO: 08.03.2020 implement correctly
    try (final SqlDao dao = this.getDao()) {
      if (!loadTags) {
        return Optional.ofNullable(dao.getArticle(id));
      }
    } catch (Exception e) {
      LOGGER.error("Error in getArticle", e);
    }
    return Optional.empty();
  }

  @Override
  public void updateArticle(final Article article) {
    try (final SqlDao dao = this.getDao()) {
      dao.updateArticle(article);
    } catch (Exception e) {
      LOGGER.error("Error in updateArticle", e);
    }
  }

  @Override
  public void updateArticleContent(final Article article) {
    try (final SqlDao dao = this.getDao()) {
      dao.updateArticleContent(article);
    } catch (Exception e) {
      LOGGER.error("Error in updateArticleContent", e);
    }
  }

  @Override
  public boolean hasArticle(final String id) {
    try (final SqlDao dao = this.getDao()) {
      return dao.hasArticle(id);
    } catch (Exception e) {
      LOGGER.error("Error in hasArticle", e);
    }
    return false;
  }

  @Override
  public void updateLocationLinks(final Article article) {
    try (final SqlDao dao = this.getDao()) {
      dao.updateLocationLinks(article);
    } catch (Exception e) {
      LOGGER.error("Error in updateLocationLinks", e);
    }
  }

  @Override
  public void updateTopicLinks(final Article article) {
    try (final SqlDao dao = this.getDao()) {
      dao.updateTopicLinks(article);
    } catch (Exception e) {
      LOGGER.error("Error in updateTopicLinks", e);
    }
  }

  @Override
  public void addLocation(final Location location) {
    try (final SqlDao dao = this.getDao()) {
      dao.addLocation(location);
    } catch (Exception e) {
      LOGGER.error("Error in addLocation", e);
    }
  }

  @Override
  public Optional<Location> getLocation(final String id) {
    try (final SqlDao dao = this.getDao()) {
      return Optional.ofNullable(dao.getLocation(id));
    } catch (Exception e) {
      LOGGER.error("Error in getLocation", e);
    }
    return Optional.empty();
  }

  @Override
  public boolean hasLocation(final String id) {
    try (final SqlDao dao = this.getDao()) {
      return dao.hasLocation(id);
    } catch (Exception e) {
      LOGGER.error("Error in hasLocation", e);
    }
    return false;
  }

  @Override
  public void addTopic(final Topic topic) {
    try (final SqlDao dao = this.getDao()) {
      dao.addTopic(topic);
    } catch (Exception e) {
      LOGGER.error("Error in addTopic", e);
    }
  }

  @Override
  public Optional<Topic> getTopic(final String id) {
    try (final SqlDao dao = this.getDao()) {
      return Optional.ofNullable(dao.getTopic(id));
    } catch (Exception e) {
      LOGGER.error("Error in hasArticle", e);
    }
    return Optional.empty();
  }

  @Override
  public boolean hasTopic(final String id) {
    try (final SqlDao dao = this.getDao()) {
      return dao.hasTopic(id);
    } catch (Exception e) {
      LOGGER.error("Error in hasTopic", e);
    }
    return false;
  }

  public PGConnectionPoolDataSource getDataSource() {
    return dataSource;
  }

  public AbstractFetcher getAbstractFetcher() {
    return this.abstractFetcher;
  }

}
