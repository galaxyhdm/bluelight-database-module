package dev.markusk.bluelight.database;

import dev.markusk.bluelight.api.objects.Article;
import dev.markusk.bluelight.api.objects.Location;
import dev.markusk.bluelight.api.objects.Topic;

import java.sql.SQLException;

public interface SqlDao extends AutoCloseable {

  void initializeTables() throws SQLException;

  void addArticle(final Article article) throws SQLException;

  Article getArticle(final String id) throws SQLException;

  void updateArticle(final Article article);

  void updateArticleContent(final Article article) throws SQLException;

  boolean hasArticle(final String id);

  void updateLocationLinks(final Article article);

  void updateTopicLinks(final Article article);

  void addLocation(final Location location) throws SQLException;

  Location getLocation(final String id) throws SQLException;

  boolean hasLocation(final String id);

  void addTopic(final Topic topic) throws SQLException;

  Topic getTopic(final String id) throws SQLException;

  boolean hasTopic(final String id);

}
