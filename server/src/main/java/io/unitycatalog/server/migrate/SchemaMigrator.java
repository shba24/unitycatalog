package io.unitycatalog.server.migrate;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.unitycatalog.server.persist.utils.HibernateConfigurator;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.net.JarURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import javax.sql.DataSource;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.CoreMigrationType;
import org.flywaydb.core.api.MigrationInfo;
import org.flywaydb.core.api.MigrationInfoService;
import org.flywaydb.core.api.MigrationVersion;
import org.flywaydb.core.api.configuration.FluentConfiguration;
import org.flywaydb.core.api.exception.FlywayValidateException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Upgrades Unity Catalog's database schema with the SQL steps of {@code db/migration/<database>}
 * and the Java steps of {@code io.unitycatalog.server.migrate.steps}, which share one sequence of
 * versions: sets up an empty database, adopts a database of the baseline release (recording version
 * 1 as the baseline) and applies the pending steps, one at a time. On PostgreSQL and MySQL, each
 * {@link #migrate()} holds a {@link RunLock} from classifying the database to the last step.
 *
 * <p>A step that adds a rule existing rows can break ships a check: {@code
 * C<version>__<description>.sql} in {@code db/migration/checks/<database>}, one query that returns
 * the rows breaking it. Before any step runs, the checks of the pending steps run, and the upgrade
 * is refused while one returns rows.
 *
 * <p>A SQL step whose change is already in the database (a run stopped before Flyway recorded it,
 * or someone applied it by hand) is recorded without running it.
 */
public final class SchemaMigrator implements AutoCloseable {

  /** What a database holds, compared with the schema steps of this build. */
  enum SchemaState {
    /** No tables, apart from Casbin's authorization rules table. */
    EMPTY,
    /** Every step of this build is applied. */
    CURRENT,
    /** Some steps of this build are not applied yet. */
    PENDING,
    /** A step newer than this build is applied: the database belongs to a newer release. */
    AHEAD,
    /** Unity Catalog's tables without Flyway's history: a database of the baseline release. */
    UNVERSIONED_UC,
    /** Unity Catalog's tables without Flyway's history, lacking some of the baseline's. */
    BEFORE_BASELINE,
    /** Tables, but none of Unity Catalog's. */
    NOT_UC
  }

  /**
   * The databases Unity Catalog's schema steps exist for; each has its folder of steps. Public for
   * the {@link DataStep}s, which pick their SQL by database.
   */
  public enum DatabaseKind {
    H2,
    POSTGRESQL,
    MYSQL;

    /**
     * The database of a JDBC URL, from its prefix.
     *
     * @throws IllegalArgumentException for a URL of any other database
     */
    static DatabaseKind fromJdbcUrl(String url) {
      for (DatabaseKind kind : values()) {
        if (url.startsWith("jdbc:" + kind.folder() + ":")) {
          return kind;
        }
      }
      // Only the scheme: the rest of a URL can hold a password.
      throw new IllegalArgumentException(
          "Unsupported database "
              + url.substring(0, Math.max(url.indexOf(':', "jdbc:".length()), 0))
              + ": use jdbc:h2:, jdbc:postgresql: or jdbc:mysql:");
    }

    /** The folder under {@code db/migration} holding this database's steps. */
    String folder() {
      return name().toLowerCase(Locale.ROOT);
    }
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(SchemaMigrator.class);
  private static final String HISTORY_TABLE = "uc_schema_history";

  /** The Java steps ({@link DataStep}s), for every database. */
  static final String JAVA_STEPS = "classpath:io/unitycatalog/server/migrate/steps";

  /**
   * The release whose schema is version 1. A database without schema history from it is adopted as
   * version 1; one from an earlier release has to be started on a {@value} server once first.
   */
  static final String BASELINE_RELEASE = "0.7.0";

  /** The schema version of {@link #BASELINE_RELEASE}, which Flyway records when it adopts one. */
  private static final String BASELINE_VERSION = "1";

  /** The baseline Flyway records on an empty database, below step 1 so that step 1 runs. */
  private static final String EMPTY_BASELINE_VERSION = "0";

  /**
   * The tables of {@link #BASELINE_RELEASE}'s schema. A database without history that lacks one is
   * from an earlier release.
   *
   * <p>TODO(#2053): compare the whole schema with the recorded baseline schema, not only its
   * tables.
   */
  private static final Set<String> BASELINE_TABLES =
      Set.of(
          "uc_catalogs",
          "uc_columns",
          "uc_credentials",
          "uc_delta_commits",
          "uc_dependencies",
          "uc_external_locations",
          "uc_function_params",
          "uc_functions",
          "uc_metastore",
          "uc_model_versions",
          "uc_properties",
          "uc_registered_models",
          "uc_schemas",
          "uc_staging_tables",
          "uc_storage_cleanup_tasks",
          "uc_tables",
          "uc_users",
          "uc_volumes");

  private static final Pattern CREATE_TABLE =
      Pattern.compile("create\\s+table\\s+(\\w+)", Pattern.CASE_INSENSITIVE);

  // The statements of the SQL steps whose change can be found in the database.
  private static final Pattern ADD_CONSTRAINT =
      Pattern.compile(
          "alter\\s+table\\s+(\\w+)\\s+add\\s+constraint\\s+(\\w+)", Pattern.CASE_INSENSITIVE);
  private static final Pattern ADD_COLUMN =
      Pattern.compile(
          "alter\\s+table\\s+(\\w+)\\s+add\\s+(?:column\\s+)?(\\w+)", Pattern.CASE_INSENSITIVE);
  private static final Pattern CREATE_INDEX =
      Pattern.compile(
          "create\\s+(?:unique\\s+)?index\\s+(\\w+)\\s+on\\s+(\\w+)", Pattern.CASE_INSENSITIVE);

  /**
   * {@code ALTER TABLE t ALTER COLUMN c [SET DATA TYPE | TYPE] type[(n)] [[NOT] NULL]} (H2,
   * PostgreSQL) or {@code ALTER TABLE t MODIFY [COLUMN] c type[(n)] [[NOT] NULL]} (MySQL), and
   * nothing after it: not a type with other parameters, such as MySQL's {@code enum (...)}, nor a
   * statement that changes more than the type, such as with {@code DEFAULT}, {@code COLLATE} or
   * {@code USING}. Group 6 is the {@code NULL} of a nullability, group 5 its {@code NOT}.
   */
  private static final Pattern CHANGE_TYPE =
      Pattern.compile(
          "alter\\s+table\\s+(\\w+)\\s+(?:alter\\s+column|modify(?:\\s+column)?)\\s+(\\w+)\\s+"
              + "(?:set\\s+data\\s+type\\s+|type\\s+)?(\\w+)"
              + "(?:\\s*\\(\\s*(\\d+)\\s*\\))?(?!\\s*\\()"
              + "(?:\\s+(not\\s+)?(null))?\\s*$",
          Pattern.CASE_INSENSITIVE);

  /** The names H2's metadata gives the types the steps name otherwise. */
  private static final Map<String, String> TYPE_NAMES =
      Map.of("clob", "character large object", "varchar", "character varying");

  /** The types the steps name without a size, since they take none. */
  private static final Set<String> UNSIZED_TYPES = Set.of("text", "clob", "mediumtext", "longtext");

  private static final Pattern CHECK = Pattern.compile("C(\\d+(?:_\\d+)*)__(.+)\\.sql");

  /** The most rows of one check that a refusal lists. */
  private static final int MAX_ROWS_LISTED = 20;

  /** The run lock's connection plus Flyway's two (PostgreSQL uses both; MySQL one). */
  private static final int POOL_SIZE = 3;

  private final DataSource dataSource;
  private final DatabaseKind kind;
  private final String[] locations;
  private final HikariDataSource ownedPool; // null: the caller's pool

  /**
   * A migrator with its own small pool on the database of {@code hibernateProperties}, which {@link
   * #close()} closes. The server's pool can then be any size.
   */
  public static SchemaMigrator fromHibernateProperties(Properties hibernateProperties) {
    HikariConfig config = HibernateConfigurator.hikariConfig(hibernateProperties);
    config.setMaximumPoolSize(POOL_SIZE);
    config.setMinimumIdle(0);
    config.setPoolName("schema-migrator");
    HikariDataSource pool = new HikariDataSource(config);
    return new SchemaMigrator(pool, DatabaseKind.fromJdbcUrl(config.getJdbcUrl()), List.of(), pool);
  }

  /** Applies this release's steps to {@code dataSource}; closing it stays with the caller. */
  public SchemaMigrator(DataSource dataSource) {
    this(dataSource, List.of());
  }

  /** Also applies the steps in {@code extraLocations} (tests' own steps). */
  SchemaMigrator(DataSource dataSource, List<String> extraLocations) {
    this(dataSource, DatabaseKind.fromJdbcUrl(jdbcUrl(dataSource)), extraLocations, null);
  }

  private SchemaMigrator(
      DataSource dataSource,
      DatabaseKind kind,
      List<String> extraLocations,
      HikariDataSource ownedPool) {
    this.dataSource = dataSource;
    this.kind = kind;
    List<String> locations = new ArrayList<>();
    locations.add("classpath:db/migration/" + kind.folder());
    locations.add(JAVA_STEPS);
    locations.addAll(extraLocations);
    this.locations = locations.toArray(String[]::new);
    this.ownedPool = ownedPool;
  }

  private static String jdbcUrl(DataSource dataSource) {
    try (Connection connection = dataSource.getConnection()) {
      return connection.getMetaData().getURL();
    } catch (SQLException e) {
      throw new IllegalStateException("Could not connect to the database: " + e.getMessage(), e);
    }
  }

  private Flyway flyway(String baselineVersion) { // null: no baselining
    return configuration(baselineVersion).load();
  }

  private FluentConfiguration configuration(String baselineVersion) {
    FluentConfiguration configuration =
        Flyway.configure()
            .dataSource(dataSource)
            .locations(locations)
            .table(HISTORY_TABLE)
            .failOnMissingLocations(true)
            .cleanDisabled(true)
            .validateMigrationNaming(true);
    if (baselineVersion != null) {
      configuration
          .baselineOnMigrate(true)
          .baselineVersion(baselineVersion)
          .baselineDescription(
              EMPTY_BASELINE_VERSION.equals(baselineVersion)
                  ? "empty"
                  : "baseline " + BASELINE_RELEASE);
    }
    return configuration;
  }

  /**
   * Under the run lock: classifies the database and runs the checks of the pending steps, then sets
   * it up, adopts it and upgrades it one step at a time, and checks that it ends at this build's
   * schema version. On PostgreSQL and MySQL, a second run at the same time waits for the first.
   * Returns the number of steps it applied, counting those it found already in the database and
   * recorded without running.
   */
  public int migrate() {
    try (RunLock lock = RunLock.acquire(dataSource, kind)) {
      SchemaState state = state();
      String from = state == SchemaState.UNVERSIONED_UC ? BASELINE_VERSION : currentVersion();
      String refusal = refusal(state);
      if (refusal == null) {
        refusal = failedChecks(from);
      }
      if (refusal != null) {
        throw new IllegalStateException(refusal);
      }
      String baseline = baselineFor(state);
      if (state == SchemaState.UNVERSIONED_UC) {
        // Before any step: a run that only records steps would record step 1 as one of them.
        flyway(baseline).baseline();
      }
      int applied = 0;
      for (MigrationInfo step : pendingSteps(from)) {
        boolean alreadyThere = isAlreadyThere(step);
        if (alreadyThere) {
          LOGGER.info(
              "Step {} ({}) is already in the database; recorded it without running it.",
              version(step),
              step.getDescription());
        }
        applied +=
            configuration(baseline)
                .target(step.getVersion())
                .skipExecutingMigrations(alreadyThere)
                .load()
                .migrate()
                .migrationsExecuted;
      }
      if (applied > 0) {
        LOGGER.info(
            "Upgraded the database schema from version {} to {} ({} step(s))",
            from == null ? "none" : from,
            currentVersion(),
            applied);
      }
      requireCurrent();
      return applied;
    }
  }

  /**
   * For {@code bin/start-uc-server --check-schema}: under the run lock, prints the database's
   * state, the pending steps and whether {@link #migrate()} would go through, running the checks of
   * the pending steps. Changes nothing.
   *
   * @return 0 when an upgrade would go through, 1 when {@link #migrate()} would refuse it
   */
  public int checkSchema(PrintStream out) {
    try (RunLock lock = RunLock.acquire(dataSource, kind)) {
      SchemaState state = state();
      String from = state == SchemaState.UNVERSIONED_UC ? BASELINE_VERSION : currentVersion();
      out.println(
          "Database: "
              + state
              + ", schema version "
              + (from == null ? "none" : from)
              + "; this release ends at "
              + latestVersion()
              + ".");
      String refusal = refusal(state);
      if (refusal == null) {
        List<MigrationInfo> pending = pendingSteps(from);
        out.println(pending.isEmpty() ? "Pending steps: none." : "Pending steps:");
        pending.forEach(m -> out.println("  " + version(m) + " " + m.getDescription()));
        refusal = failedChecks(from);
      }
      out.println(refusal == null ? "The upgrade would go through." : refusal);
      return refusal == null ? 0 : 1;
    }
  }

  /**
   * Classifies the database against this build's steps. Changes nothing and does not take the run
   * lock, so a run in progress can change the answer.
   */
  SchemaState state() {
    Set<String> tables = tableNamesLowerCase();
    tables.remove("casbin_rule"); // Casbin's adapter creates it on its own
    if (tables.contains(HISTORY_TABLE)) {
      MigrationInfoService info = flyway(null).info();
      String current = version(info.current());
      if (current == null) {
        return SchemaState.PENDING;
      }
      int cmp =
          MigrationVersion.fromVersion(current)
              .compareTo(MigrationVersion.fromVersion(latestVersion(info)));
      return cmp == 0 ? SchemaState.CURRENT : cmp > 0 ? SchemaState.AHEAD : SchemaState.PENDING;
    }
    if (tables.isEmpty()) {
      return SchemaState.EMPTY;
    }
    if (!tables.contains("uc_catalogs")) {
      return SchemaState.NOT_UC;
    }
    return tables.containsAll(BASELINE_TABLES)
        ? SchemaState.UNVERSIONED_UC
        : SchemaState.BEFORE_BASELINE;
  }

  /** The highest applied version, {@code null} without history. */
  String currentVersion() {
    return version(flyway(null).info().current());
  }

  /** The highest step in this build. */
  String latestVersion() {
    return latestVersion(flyway(null).info());
  }

  private static String latestVersion(MigrationInfoService info) {
    return Arrays.stream(info.all())
        .filter(m -> m.getState().isResolved() && m.getVersion() != null)
        .map(MigrationInfo::getVersion)
        .max(MigrationVersion::compareTo)
        .orElseThrow()
        .getVersion();
  }

  /**
   * The baseline Flyway records before the first step: below step 1 on an empty database so step 1
   * runs, at 1 on a baseline-release database so step 1 is skipped, and none once history exists.
   */
  private static String baselineFor(SchemaState state) {
    return switch (state) {
      case EMPTY -> EMPTY_BASELINE_VERSION;
      case UNVERSIONED_UC -> BASELINE_VERSION;
      default -> null;
    };
  }

  /**
   * Why {@link #migrate()} refuses a database in {@code state}, or {@code null} when it goes on to
   * the checks: an unsupported database, or a history Flyway refuses (such as a failed step).
   */
  private String refusal(SchemaState state) {
    return switch (state) {
      case NOT_UC ->
          "The database has tables but none of Unity Catalog's: not a Unity Catalog database."
              + " Point the server at an empty database or a Unity Catalog one.";
      case AHEAD ->
          "The schema is newer than this release (version "
              + currentVersion()
              + ", this release ends at "
              + latestVersion()
              + "). Run the newer release.";
      case BEFORE_BASELINE ->
          "The database is from a release before "
              + BASELINE_RELEASE
              + " (missing tables: "
              + String.join(", ", missingBaselineTables())
              + "). Start a "
              + BASELINE_RELEASE
              + " server on it once, then start this release.";
      case CURRENT, PENDING -> historyRefusal();
      default -> null;
    };
  }

  /**
   * Flyway's refusal of the schema history, such as a failed step, or {@code null}: the error each
   * step's run would stop with, found before any step or check runs. Pending steps are no error.
   */
  private String historyRefusal() {
    try {
      configuration(null).ignoreMigrationPatterns("*:pending").load().validate();
      return null;
    } catch (FlywayValidateException e) {
      return e.getMessage();
    }
  }

  /** Throws unless the database ends at this build's schema version. */
  private void requireCurrent() {
    SchemaState state = state();
    if (state != SchemaState.CURRENT) {
      throw new IllegalStateException(
          "The database schema is "
              + state
              + " after migration; this release needs version "
              + latestVersion());
    }
  }

  /** The steps above version {@code from} ({@code null}: every step), in order. */
  private List<MigrationInfo> pendingSteps(String from) {
    return Arrays.stream(flyway(null).info().pending())
        .filter(
            m -> from == null || m.getVersion().compareTo(MigrationVersion.fromVersion(from)) > 0)
        .toList();
  }

  /**
   * Whether the change of a SQL step is already in the database, so that running it would fail: a
   * run stopped between the step and its history row, or someone applied it by hand. Recognises a
   * step by its one statement; any other step runs.
   */
  private boolean isAlreadyThere(MigrationInfo step) {
    if (step.getType() != CoreMigrationType.SQL) {
      return false;
    }
    String statement = stepStatement(step);
    if (statement.contains(";") // more than one statement, such as the baseline step
        || hasSeveralChanges(statement)) {
      return false;
    }
    try (Connection connection = dataSource.getConnection()) {
      return isAlreadyThere(statement, connection);
    } catch (SQLException e) {
      throw new IllegalStateException(
          "Could not check whether step "
              + version(step)
              + " is already in the database: "
              + e.getMessage(),
          e);
    }
  }

  private boolean isAlreadyThere(String statement, Connection connection) throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    String catalog = connection.getCatalog();
    String schema = connection.getSchema();
    Matcher createTable = CREATE_TABLE.matcher(statement);
    Matcher addConstraint = ADD_CONSTRAINT.matcher(statement);
    Matcher addColumn = ADD_COLUMN.matcher(statement);
    Matcher createIndex = CREATE_INDEX.matcher(statement);
    Matcher changeType = CHANGE_TYPE.matcher(statement);
    if (createTable.lookingAt()) {
      return tableName(metaData, catalog, schema, createTable.group(1)) != null;
    } else if (addConstraint.lookingAt()) {
      return hasConstraint(connection, addConstraint.group(1), addConstraint.group(2));
    } else if (addColumn.lookingAt()) {
      String table = tableName(metaData, catalog, schema, addColumn.group(1));
      return table != null
          && names(metaData.getColumns(catalog, schema, table, null), "COLUMN_NAME")
              .contains(addColumn.group(2).toLowerCase(Locale.ROOT));
    } else if (createIndex.lookingAt()) {
      String table = tableName(metaData, catalog, schema, createIndex.group(2));
      return table != null
          && names(metaData.getIndexInfo(catalog, schema, table, false, false), "INDEX_NAME")
              .contains(createIndex.group(1).toLowerCase(Locale.ROOT));
    } else if (changeType.lookingAt()) {
      if (changeType.group(4) == null
          && !UNSIZED_TYPES.contains(changeType.group(3).toLowerCase(Locale.ROOT))) {
        return false; // without a size, the type may be a sized one at its default size
      }
      // MySQL's MODIFY and H2's ALTER COLUMN c type (without SET DATA TYPE or TYPE) restate the
      // column: they drop its default, and MySQL's makes it nullable unless it says NOT NULL.
      boolean restates = statement.substring(changeType.end(2), changeType.start(3)).isBlank();
      Boolean nullable =
          changeType.group(6) != null
              ? Boolean.valueOf(changeType.group(5) == null)
              : restates && kind == DatabaseKind.MYSQL ? Boolean.TRUE : null;
      String table = tableName(metaData, catalog, schema, changeType.group(1));
      return table != null
          && hasType(
              metaData.getColumns(catalog, schema, table, null),
              changeType.group(2),
              changeType.group(3),
              changeType.group(4),
              nullable,
              restates);
    }
    return false;
  }

  /**
   * Whether the column {@code column} of the columns {@code rs} (which this closes) has the type
   * {@code type} and, unless {@code null}, the size {@code size} and the nullability {@code
   * nullable}, and when {@code noDefault}, no default.
   */
  private static boolean hasType(
      ResultSet rs, String column, String type, String size, Boolean nullable, boolean noDefault)
      throws SQLException {
    try (rs) {
      while (rs.next()) {
        if (rs.getString("COLUMN_NAME").equalsIgnoreCase(column)) {
          return typeName(rs.getString("TYPE_NAME")).equals(typeName(type))
              && (size == null || rs.getInt("COLUMN_SIZE") == Integer.parseInt(size))
              && (nullable == null || nullable == "YES".equals(rs.getString("IS_NULLABLE")))
              && (!noDefault || rs.getString("COLUMN_DEF") == null);
        }
      }
    }
    return false;
  }

  /** A type's name, lower-cased, with H2's names for the short ones the steps use. */
  private static String typeName(String type) {
    String name = type.toLowerCase(Locale.ROOT);
    return TYPE_NAMES.getOrDefault(name, name);
  }

  /**
   * Whether a statement makes several changes, such as {@code ADD COLUMN a INT, ADD COLUMN b INT}:
   * it has a comma outside parentheses and quotes.
   */
  private static boolean hasSeveralChanges(String statement) {
    int depth = 0;
    char quote = 0;
    for (char c : statement.toCharArray()) {
      if (quote != 0) {
        quote = c == quote ? 0 : quote;
      } else if (c == '\'' || c == '"' || c == '`') {
        quote = c;
      } else if (c == '(' || c == ')') {
        depth += c == '(' ? 1 : -1;
      } else if (c == ',' && depth == 0) {
        return true;
      }
    }
    return false;
  }

  /** The name of {@code table} as the database stores it, or {@code null} when it is missing. */
  private static String tableName(
      DatabaseMetaData metaData, String catalog, String schema, String table) throws SQLException {
    try (ResultSet rs = metaData.getTables(catalog, schema, null, new String[] {"TABLE"})) {
      while (rs.next()) {
        if (rs.getString("TABLE_NAME").equalsIgnoreCase(table)) {
          return rs.getString("TABLE_NAME");
        }
      }
    }
    return null;
  }

  /** The lower-cased values of column {@code label} of {@code rs}, which this closes. */
  private static Set<String> names(ResultSet rs, String label) throws SQLException {
    Set<String> names = new HashSet<>();
    try (rs) {
      while (rs.next()) {
        if (rs.getString(label) != null) {
          names.add(rs.getString(label).toLowerCase(Locale.ROOT));
        }
      }
    }
    return names;
  }

  /** Whether {@code table} has a key, foreign key or CHECK constraint named {@code constraint}. */
  private boolean hasConstraint(Connection connection, String table, String constraint)
      throws SQLException {
    String sql =
        "SELECT 1 FROM information_schema.table_constraints WHERE table_schema = "
            + (kind == DatabaseKind.MYSQL ? "DATABASE()" : "current_schema")
            + " AND LOWER(table_name) = ? AND LOWER(constraint_name) = ?";
    try (PreparedStatement query = connection.prepareStatement(sql)) {
      query.setString(1, table.toLowerCase(Locale.ROOT));
      query.setString(2, constraint.toLowerCase(Locale.ROOT));
      try (ResultSet rs = query.executeQuery()) {
        return rs.next();
      }
    }
  }

  /** A SQL step's statements without comments or the last semicolon. */
  private String stepStatement(MigrationInfo step) {
    for (String location : locations) {
      if (location.startsWith("classpath:") && !location.equals(JAVA_STEPS)) {
        String folder = location.substring("classpath:".length()).replaceAll("/+$", "");
        InputStream in =
            SchemaMigrator.class
                .getClassLoader()
                .getResourceAsStream(folder + "/" + step.getScript());
        if (in != null) {
          return read(in).replaceAll("--[^\n]*", "").strip().replaceAll(";$", "");
        }
      }
    }
    throw new IllegalStateException("Could not find the step " + step.getScript());
  }

  /**
   * Runs the check of every step above version {@code from}, each in a read-only transaction of its
   * own, and returns the refusal when any finds rows, or {@code null}. Runs none without a version:
   * an empty database has no rows to check.
   */
  private String failedChecks(String from) {
    if (from == null) {
      return null;
    }
    MigrationVersion current = MigrationVersion.fromVersion(from);
    List<String> failures = new ArrayList<>();
    for (URL check : checks()) {
      MigrationVersion version = checkVersion(check);
      if (version.compareTo(current) > 0) {
        String description = checkName(check).group(2).replace('_', ' ');
        String rows = rowsFound(check, "Check " + version + " (" + description + ")");
        if (rows != null) {
          failures.add(rows);
        }
      }
    }
    if (failures.isEmpty()) {
      return null;
    }
    return "The upgrade is stopped before any change: "
        + failures.size()
        + (failures.size() == 1 ? " check found" : " checks found")
        + " rows that break a rule of this release.\n"
        + String.join("", failures)
        + "Fix them as docs/server/upgrades.md shows (section \"Rows that block an upgrade\"),"
        + " then start the server again.";
  }

  /**
   * The checks of the SQL step locations, by version: those of {@code <base>/<database>} are in
   * {@code <base>/checks/<database>}.
   */
  private List<URL> checks() {
    // By classpath path: like a class, a file on the classpath twice counts once, the first copy.
    Map<String, URL> checks = new LinkedHashMap<>();
    for (String location : locations) {
      if (location.equals(JAVA_STEPS) || !location.startsWith("classpath:")) {
        continue;
      }
      String folder = location.substring("classpath:".length()).replaceAll("/+$", "");
      int slash = folder.lastIndexOf('/');
      String path = folder.substring(0, slash + 1) + "checks/" + folder.substring(slash + 1);
      try {
        for (URL url : Collections.list(SchemaMigrator.class.getClassLoader().getResources(path))) {
          for (String file : fileNames(url, path)) {
            if (CHECK.matcher(file).matches()) {
              checks.putIfAbsent(path + "/" + file, URI.create(url + "/" + file).toURL());
            }
          }
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
    return checks.values().stream()
        .sorted(Comparator.comparing(SchemaMigrator::checkVersion))
        .toList();
  }

  /** The names of the files in the classpath folder {@code path}, found at {@code url}. */
  private static List<String> fileNames(URL url, String path) throws IOException {
    if (url.getProtocol().equals("jar")) {
      JarURLConnection connection = (JarURLConnection) url.openConnection();
      connection.setUseCaches(false); // a JarFile of its own, to close
      try (JarFile jar = connection.getJarFile()) {
        String prefix = path + "/";
        return jar.stream()
            .map(JarEntry::getName)
            .filter(n -> n.startsWith(prefix) && n.indexOf('/', prefix.length()) < 0)
            .map(n -> n.substring(prefix.length()))
            .toList();
      }
    }
    try (Stream<Path> files = Files.list(Path.of(URI.create(url.toString())))) {
      return files.map(f -> f.getFileName().toString()).toList();
    }
  }

  /** A check's file name matched by {@link #CHECK}: group 1 is the version, 2 the description. */
  private static Matcher checkName(URL check) {
    String path = check.getPath();
    Matcher name = CHECK.matcher(path.substring(path.lastIndexOf('/') + 1));
    name.matches();
    return name;
  }

  /** The version of a check: "C1_1__name.sql" is 1.1. */
  private static MigrationVersion checkVersion(URL check) {
    return MigrationVersion.fromVersion(checkName(check).group(1).replace('_', '.'));
  }

  /**
   * Runs a check in a read-only transaction: the rows it found, at most {@value #MAX_ROWS_LISTED}
   * as {@code column=value}, under {@code title}, or {@code null} when it found none.
   */
  private String rowsFound(URL check, String title) {
    String sql;
    try (InputStream in = check.openStream()) {
      sql = new String(in.readAllBytes(), StandardCharsets.UTF_8).strip().replaceAll(";$", "");
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    try (Connection connection = dataSource.getConnection()) {
      connection.setReadOnly(true);
      connection.setAutoCommit(false);
      try (Statement statement = connection.createStatement();
          ResultSet rs = statement.executeQuery(sql)) {
        StringBuilder rows = new StringBuilder();
        int count = 0;
        while (rs.next()) {
          if (++count <= MAX_ROWS_LISTED) {
            rows.append("  ").append(row(rs)).append('\n');
          }
        }
        if (count > MAX_ROWS_LISTED) {
          rows.append("  ... and ").append(count - MAX_ROWS_LISTED).append(" more\n");
        }
        return count == 0
            ? null
            : title + " found " + count + (count == 1 ? " row:\n" : " rows:\n") + rows;
      } finally {
        connection.rollback();
      }
    } catch (SQLException e) {
      throw new IllegalStateException(title + " failed: " + e.getMessage(), e);
    }
  }

  /** The current row as {@code column=value, ...}, in the query's column order. */
  private static String row(ResultSet rs) throws SQLException {
    ResultSetMetaData columns = rs.getMetaData();
    List<String> values = new ArrayList<>();
    for (int i = 1; i <= columns.getColumnCount(); i++) {
      values.add(columns.getColumnLabel(i).toLowerCase(Locale.ROOT) + "=" + rs.getString(i));
    }
    return String.join(", ", values);
  }

  /** Reads and closes {@code in}. */
  private static String read(InputStream in) {
    try (in) {
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private Set<String> missingBaselineTables() {
    Set<String> missing = new TreeSet<>(BASELINE_TABLES);
    missing.removeAll(tableNamesLowerCase());
    return missing;
  }

  private Set<String> tableNamesLowerCase() {
    Set<String> tables = new HashSet<>();
    try (Connection connection = dataSource.getConnection();
        ResultSet rs =
            connection
                .getMetaData()
                .getTables(
                    connection.getCatalog(),
                    connection.getSchema(),
                    null,
                    new String[] {"TABLE"})) {
      while (rs.next()) {
        tables.add(rs.getString("TABLE_NAME").toLowerCase(Locale.ROOT));
      }
    } catch (SQLException e) {
      throw new IllegalStateException("Could not list the database's tables: " + e.getMessage(), e);
    }
    return tables;
  }

  private static String version(MigrationInfo info) {
    return info == null || info.getVersion() == null ? null : info.getVersion().getVersion();
  }

  /** Closes the migrator's own pool, if it has one. */
  @Override
  public void close() {
    if (ownedPool != null) {
      ownedPool.close();
    }
  }

  /**
   * One lock per database for a whole migration run, so two servers never run the steps at the same
   * time; Flyway's own lock is released between steps. PostgreSQL takes {@code pg_advisory_lock} on
   * a fixed key (advisory locks are per database), MySQL {@code GET_LOCK} on a name derived from
   * the database's (MySQL's locks are per server). The lock is held on one connection until {@link
   * #close()}. H2 takes a lock inside this JVM, keyed by the database: an H2 file database is
   * opened by one process.
   */
  static final class RunLock implements AutoCloseable {

    /** Any fixed key; Flyway's own advisory lock uses a different one. */
    private static final long POSTGRES_KEY = 0x75635f6d69677261L; // "uc_migra"

    /**
     * The lock name, as SQL. MySQL's locks are per server, so the name includes the database; SHA1
     * keeps it within MySQL's 64-character limit.
     */
    private static final String MYSQL_NAME = "CONCAT('uc_migrate.', SHA1(DATABASE()))";

    private static final ConcurrentMap<String, ReentrantLock> H2_LOCKS = new ConcurrentHashMap<>();

    private final Connection connection; // PostgreSQL and MySQL
    private final String unlockSql;
    private final ReentrantLock h2Lock; // H2

    private RunLock(Connection connection, String unlockSql, ReentrantLock h2Lock) {
      this.connection = connection;
      this.unlockSql = unlockSql;
      this.h2Lock = h2Lock;
    }

    /**
     * Blocks until this run holds the lock, logging once when another run holds it. Throws when the
     * database fails the lock call or does not grant the lock, so the run never starts unlocked.
     */
    static RunLock acquire(DataSource dataSource, DatabaseKind kind) {
      if (kind == DatabaseKind.H2) {
        ReentrantLock lock =
            H2_LOCKS.computeIfAbsent(h2Key(dataSource), key -> new ReentrantLock());
        if (!lock.tryLock()) {
          logWaiting();
          lock.lock();
        }
        return new RunLock(null, null, lock);
      }
      boolean postgres = kind == DatabaseKind.POSTGRESQL;
      String tryLock =
          postgres
              ? "SELECT pg_try_advisory_lock(" + POSTGRES_KEY + ")"
              : "SELECT GET_LOCK(" + MYSQL_NAME + ", 0)";
      String lock =
          postgres
              ? "SELECT true FROM pg_advisory_lock(" + POSTGRES_KEY + ")"
              : "SELECT GET_LOCK(" + MYSQL_NAME + ", -1)";
      String unlock =
          postgres
              ? "SELECT pg_advisory_unlock(" + POSTGRES_KEY + ")"
              : "SELECT RELEASE_LOCK(" + MYSQL_NAME + ")";
      Connection connection = null;
      try {
        connection = dataSource.getConnection();
        // Outside a transaction, so the run doesn't keep one open while it holds the lock.
        connection.setAutoCommit(true);
        if (!isTrue(connection, tryLock)) {
          logWaiting();
          if (!isTrue(connection, lock)) {
            throw new SQLException("the database did not grant it");
          }
        }
        return new RunLock(connection, unlock, null);
      } catch (SQLException e) {
        close(connection);
        throw new IllegalStateException(
            "Could not take the schema migration lock: " + e.getMessage(), e);
      }
    }

    /**
     * Releases the lock. Throws when the release fails, so the server stops instead of starting
     * with the lock state unknown. The database connection underneath is then closed too, which
     * ends its session and so releases the lock; closing a pooled connection only returns it.
     */
    @Override
    public void close() {
      if (h2Lock != null) {
        h2Lock.unlock();
        return;
      }
      try {
        if (!isTrue(connection, unlockSql)) {
          throw new SQLException("the database did not hold it for this run");
        }
      } catch (SQLException e) {
        closeUnderlying(connection);
        throw new IllegalStateException(
            "Could not release the schema migration lock: "
                + e.getMessage()
                + ". The database releases it when the connection closes; start the server again.",
            e);
      } finally {
        close(connection);
      }
    }

    /** The number of runs waiting for this H2 lock (tests). */
    int h2Waiting() {
      return h2Lock.getQueueLength();
    }

    private static void logWaiting() {
      LOGGER.info("Another server is migrating the schema; waiting for it to finish.");
    }

    /** The database file's path, or the name of an in-memory database. */
    private static String h2Key(DataSource dataSource) {
      try (Connection connection = dataSource.getConnection();
          Statement statement = connection.createStatement();
          ResultSet rs = statement.executeQuery("SELECT COALESCE(DATABASE_PATH(), DATABASE())")) {
        rs.next();
        return rs.getString(1);
      } catch (SQLException e) {
        throw new IllegalStateException(
            "Could not take the schema migration lock: " + e.getMessage(), e);
      }
    }

    /**
     * Runs a lock call; whether it returned true (MySQL's 1). NULL, MySQL's error result, throws.
     */
    private static boolean isTrue(Connection connection, String sql) throws SQLException {
      try (Statement statement = connection.createStatement();
          ResultSet rs = statement.executeQuery(sql)) {
        if (!rs.next()) {
          return false;
        }
        boolean value = rs.getBoolean(1);
        if (rs.wasNull()) {
          throw new SQLException("the lock call returned NULL");
        }
        return value;
      }
    }

    private static void closeUnderlying(Connection connection) {
      try {
        connection.unwrap(Connection.class).close();
      } catch (SQLException e) {
        LOGGER.warn(
            "Could not close the schema migration lock's database connection; the lock stays held"
                + " until that database session ends",
            e);
      }
    }

    private static void close(Connection connection) {
      try {
        if (connection != null) {
          connection.close();
        }
      } catch (SQLException e) {
        LOGGER.debug("Could not close the schema migration lock's connection", e);
      }
    }
  }
}
