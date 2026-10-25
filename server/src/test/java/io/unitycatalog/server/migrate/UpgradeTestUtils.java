package io.unitycatalog.server.migrate;

import io.unitycatalog.server.migrate.SchemaMigrator.DatabaseKind;
import io.unitycatalog.server.testdatabase.TestDatabase;
import io.unitycatalog.server.testdatabase.TestDatabases;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;

/** Databases and statements the schema migration tests share. */
final class UpgradeTestUtils {

  private UpgradeTestUtils() {}

  /** Builds a database of the baseline release: its schema without Flyway's history. */
  static void create07Schema(TestDatabase db) {
    String path = "db/migration/" + kind(db).folder() + "/V1__baseline_0_7.sql";
    String sql;
    try (InputStream in = UpgradeTestUtils.class.getClassLoader().getResourceAsStream(path)) {
      sql = new String(in.readAllBytes(), StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
    String withoutComments = sql.replaceAll("(?m)^\\s*--.*$", "");
    for (String statement : withoutComments.split(";")) {
      if (!statement.isBlank()) {
        execute(db, statement);
      }
    }
  }

  /** The schema the steps build on a new empty database. */
  static SchemaSnapshot freshInstallSnapshot() {
    try (TestDatabase fresh = TestDatabases.newEmptyDatabase()) {
      new SchemaMigrator(fresh.dataSource()).migrate();
      return SchemaSnapshot.of(fresh.dataSource());
    }
  }

  static DatabaseKind kind(TestDatabase db) {
    return DatabaseKind.fromJdbcUrl(db.jdbcUrl());
  }

  /** The quote for identifiers: Flyway's history table has quoted lower-case column names. */
  static String identifierQuote(DataSource dataSource) {
    try (Connection connection = dataSource.getConnection()) {
      return connection.getMetaData().getIdentifierQuoteString();
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * The rows of Flyway's history with a version, in order, each as {@code "<version> <type>
   * <success>"}.
   */
  static List<String> history(TestDatabase db) {
    String sql =
        ("SELECT %1$sversion%1$s, %1$stype%1$s, %1$ssuccess%1$s FROM %1$suc_schema_history%1$s"
                + " WHERE %1$sversion%1$s IS NOT NULL ORDER BY %1$sinstalled_rank%1$s")
            .formatted(identifierQuote(db.dataSource()));
    List<String> rows = new ArrayList<>();
    try (Connection connection = db.dataSource().getConnection();
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(sql)) {
      while (rs.next()) {
        rows.add(rs.getString(1) + " " + rs.getString(2) + " " + rs.getBoolean(3));
      }
    } catch (SQLException e) {
      throw new IllegalStateException(sql, e);
    }
    return rows;
  }

  static void execute(TestDatabase db, String sql) {
    try (Connection connection = db.dataSource().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(sql);
    } catch (SQLException e) {
      throw new IllegalStateException(sql, e);
    }
  }
}
