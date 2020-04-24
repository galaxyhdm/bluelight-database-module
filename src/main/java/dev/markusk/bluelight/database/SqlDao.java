package dev.markusk.bluelight.database;

import dev.markusk.bluelight.api.objects.Article;
import dev.markusk.bluelight.api.objects.Location;
import dev.markusk.bluelight.api.objects.Topic;

import java.sql.SQLException;
import java.util.List;
import java.util.Set;

public interface SqlDao extends AutoCloseable {

  void initializeTables() throws SQLException;

  void addArticle(final Article article) throws SQLException;

  Article getArticle(final String id) throws SQLException;

  List<Article> getArticles() throws SQLException;

  void updateArticle(final Article article) throws SQLException;

  void updateArticleContent(final Article article) throws SQLException;

  boolean hasArticle(final String id) throws SQLException;

  void updateLocationLinks(final Article article) throws SQLException;

  Set<Location> getLocations(final String articleId) throws SQLException;

  void updateTopicLinks(final Article article) throws SQLException;

  Set<Topic> getTopics(final String articleId) throws SQLException;

  void addLocation(final Location location) throws SQLException;

  Location getLocation(final String id) throws SQLException;

  boolean hasLocation(final String id) throws SQLException;

  boolean hasLocationByName(final String locationName) throws SQLException;

  void addTopic(final Topic topic) throws SQLException;

  Topic getTopic(final String id) throws SQLException;

  boolean hasTopic(final String id) throws SQLException;

  boolean hasTopicByName(final String topicName) throws SQLException;

}
