package io.unitycatalog.server.service.delta;

import io.unitycatalog.server.delta.model.DeltaAddCommitUpdate;
import io.unitycatalog.server.delta.model.DeltaArrayType;
import io.unitycatalog.server.delta.model.DeltaAssertEtag;
import io.unitycatalog.server.delta.model.DeltaAssertTableUUID;
import io.unitycatalog.server.delta.model.DeltaCommit;
import io.unitycatalog.server.delta.model.DeltaDataType;
import io.unitycatalog.server.delta.model.DeltaDomainMetadataUpdates;
import io.unitycatalog.server.delta.model.DeltaMapType;
import io.unitycatalog.server.delta.model.DeltaProtocol;
import io.unitycatalog.server.delta.model.DeltaRemoveDomainMetadataUpdate;
import io.unitycatalog.server.delta.model.DeltaRemovePropertiesUpdate;
import io.unitycatalog.server.delta.model.DeltaSetDomainMetadataUpdate;
import io.unitycatalog.server.delta.model.DeltaSetLatestBackfilledVersionUpdate;
import io.unitycatalog.server.delta.model.DeltaSetPartitionColumnsUpdate;
import io.unitycatalog.server.delta.model.DeltaSetPropertiesUpdate;
import io.unitycatalog.server.delta.model.DeltaSetProtocolUpdate;
import io.unitycatalog.server.delta.model.DeltaSetSchemaUpdate;
import io.unitycatalog.server.delta.model.DeltaSetTableCommentUpdate;
import io.unitycatalog.server.delta.model.DeltaStructField;
import io.unitycatalog.server.delta.model.DeltaStructFieldMetadata;
import io.unitycatalog.server.delta.model.DeltaStructType;
import io.unitycatalog.server.delta.model.DeltaTableRequirement;
import io.unitycatalog.server.delta.model.DeltaTableUpdate;
import io.unitycatalog.server.delta.model.DeltaUpdateSnapshotVersionUpdate;
import io.unitycatalog.server.delta.model.DeltaUpdateTableRequest;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.ColumnInfo;
import io.unitycatalog.server.model.DeltaCommitInfo;
import io.unitycatalog.server.model.TableType;
import io.unitycatalog.server.persist.MutablePropertyMap;
import io.unitycatalog.server.persist.dao.ColumnInfoDAO;
import io.unitycatalog.server.persist.dao.TableInfoDAO;
import io.unitycatalog.server.service.delta.DeltaConsts.DomainMetadataNames;
import io.unitycatalog.server.service.delta.DeltaConsts.TableProperties;
import io.unitycatalog.server.utils.ColumnUtils;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.ValidationUtils;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.hibernate.Session;

/**
 * Translates a {@link DeltaUpdateTableRequest} into in-memory mutations on a {@link TableInfoDAO}
 * and {@link MutablePropertyMap}. Three phases, each separately callable:
 *
 * <ol>
 *   <li>{@link #collectRequest} -- pre-transaction shape checks; classify by subtype.
 *   <li>{@link #checkTableUuidRequirement} / {@link #checkEtagRequirement} -- {@code assert-*}
 *       requirements against the loaded DAO.
 *   <li>{@link #applyUpdates} -- dispatch each action onto the DAO and property map.
 * </ol>
 *
 * <p>The mapper does no DB I/O; the caller ({@link io.unitycatalog.server.persist.TableRepository})
 * loads state once and flushes the diff.
 */
public final class DeltaUpdateTableMapper {

  private DeltaUpdateTableMapper() {}

  // ---------------------------------------------------------------------- collection

  public record CollectedRequest(CollectedRequirements requirements, CollectedUpdates updates) {}

  /**
   * Classify and shape-check the request. {@code assert-table-uuid} is mandatory: without it a
   * client with a cached three-part name could silently commit to a freshly-recreated table.
   */
  public static CollectedRequest collectRequest(DeltaUpdateTableRequest request) {
    if (request == null) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Request body is required.");
    }
    List<DeltaTableUpdate> updates = request.getUpdates();
    if (updates == null || updates.isEmpty()) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "At least one update is required.");
    }
    CollectedRequirements requirements = collectRequirements(request.getRequirements());
    if (requirements.assertTableUuid.isEmpty()) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT, "assert-table-uuid requirement is required.");
    }
    CollectedUpdates collected = collectUpdates(updates);
    checkUpdateOverlaps(collected);
    return new CollectedRequest(requirements, collected);
  }

  /**
   * Reject requests that touch the same property key (or domain name) from both a set and a remove
   * action. The mapper's canonical apply order would resolve such an overlap deterministically
   * (remove runs after set), but the intent is contradictory -- almost always a client bug -- so
   * fail fast at shape-check time rather than silently dropping one of the two.
   */
  private static void checkUpdateOverlaps(CollectedUpdates u) {
    if (u.setProperties.isPresent() && u.removeProperties.isPresent()) {
      Map<String, String> setMap = u.setProperties.get().getUpdates();
      List<String> removeList = u.removeProperties.get().getRemovals();
      Set<String> overlap = intersect(setMap == null ? null : setMap.keySet(), removeList);
      if (!overlap.isEmpty()) {
        throw new BaseException(
            ErrorCode.INVALID_ARGUMENT,
            "set-properties and remove-properties overlap on keys: " + overlap);
      }
    }
    if (u.setDomainMetadata.isPresent() && u.removeDomainMetadata.isPresent()) {
      Set<String> setDomains = domainsSetIn(u.setDomainMetadata.get().getUpdates());
      List<String> removeList = u.removeDomainMetadata.get().getDomains();
      Set<String> overlap = intersect(setDomains, removeList);
      if (!overlap.isEmpty()) {
        throw new BaseException(
            ErrorCode.INVALID_ARGUMENT,
            "set-domain-metadata and remove-domain-metadata overlap on domains: " + overlap);
      }
    }
  }

  /** Domain names with a non-null entry in {@code updates}. */
  private static Set<String> domainsSetIn(DeltaDomainMetadataUpdates updates) {
    if (updates == null) {
      return Set.of();
    }
    Set<String> domains = new HashSet<>();
    if (updates.getDeltaClustering() != null) {
      domains.add(DomainMetadataNames.CLUSTERING);
    }
    if (updates.getDeltaRowTracking() != null) {
      domains.add(DomainMetadataNames.ROW_TRACKING);
    }
    return domains;
  }

  /** Set-intersection that tolerates null inputs and yields a sorted view for stable messages. */
  private static Set<String> intersect(Set<String> a, List<String> b) {
    if (a == null || a.isEmpty() || b == null || b.isEmpty()) {
      return Set.of();
    }
    Set<String> out = new TreeSet<>(a);
    out.retainAll(b);
    return out;
  }

  /** One slot per requirement subtype; enforces at-most-one per request. */
  public static final class CollectedRequirements {
    private Optional<DeltaAssertTableUUID> assertTableUuid = Optional.empty();
    private Optional<DeltaAssertEtag> assertEtag = Optional.empty();

    void putOnce(DeltaTableRequirement req) {
      if (req instanceof DeltaAssertTableUUID u) {
        assertTableUuid = fillOnce(assertTableUuid, u, DeltaTableRequirement::getType);
      } else if (req instanceof DeltaAssertEtag e) {
        assertEtag = fillOnce(assertEtag, e, DeltaTableRequirement::getType);
      } else {
        throw new BaseException(
            ErrorCode.INVALID_ARGUMENT,
            "Unknown requirement type: " + (req == null ? "null" : req.getType()));
      }
    }
  }

  /** One slot per update subtype; enforces at-most-one per request. */
  public static final class CollectedUpdates {
    private Optional<DeltaSetPropertiesUpdate> setProperties = Optional.empty();
    private Optional<DeltaRemovePropertiesUpdate> removeProperties = Optional.empty();
    private Optional<DeltaSetProtocolUpdate> setProtocol = Optional.empty();
    private Optional<DeltaSetSchemaUpdate> setSchema = Optional.empty();
    private Optional<DeltaSetPartitionColumnsUpdate> setPartitionColumns = Optional.empty();
    private Optional<DeltaSetTableCommentUpdate> setTableComment = Optional.empty();
    private Optional<DeltaSetDomainMetadataUpdate> setDomainMetadata = Optional.empty();
    private Optional<DeltaRemoveDomainMetadataUpdate> removeDomainMetadata = Optional.empty();
    private Optional<DeltaUpdateSnapshotVersionUpdate> updateSnapshotVersion = Optional.empty();
    private Optional<DeltaAddCommitUpdate> addCommit = Optional.empty();
    private Optional<DeltaSetLatestBackfilledVersionUpdate> setLatestBackfilledVersion =
        Optional.empty();

    /**
     * True if this request carries at least one MANAGED-applicable metadata-changing action
     * (properties, protocol, schema, partition columns, comment, domain metadata). Used to gate the
     * auto-stamping of {@code delta.lastUpdateVersion} / {@code delta.lastCommitTimestamp} on a
     * MANAGED {@code add-commit}. {@code update-metadata-snapshot-version} is intentionally
     * omitted: it is EXTERNAL-only and so can never co-occur with {@code add-commit}.
     *
     * <p>A {@code set-domain-metadata} that touches only the {@code delta.rowTracking} domain does
     * not count: the Delta protocol requires writers to advance the row-tracking high-water mark in
     * every commit that assigns fresh row IDs, so clients mirror it alongside otherwise data-only
     * {@code add-commit}s. It is per-commit snapshot bookkeeping, not a metadata change; counting
     * it would advance {@code updatedAt} and the {@code delta.lastUpdateVersion} stamp on every
     * data commit of a row-tracking table. The high-water-mark property itself is still persisted.
     */
    boolean hasManagedTableMetadataChange() {
      return setProperties.isPresent()
          || removeProperties.isPresent()
          || setProtocol.isPresent()
          || setSchema.isPresent()
          || setPartitionColumns.isPresent()
          || setTableComment.isPresent()
          || setsDomainMetadataBeyondRowTracking()
          || removeDomainMetadata.isPresent();
    }

    /**
     * True if {@code set-domain-metadata} is present and sets any domain other than {@code
     * delta.rowTracking}. Extend this check when new domains are added to {@link
     * DeltaDomainMetadataUpdates}.
     */
    private boolean setsDomainMetadataBeyondRowTracking() {
      if (setDomainMetadata.isEmpty()) {
        return false;
      }
      DeltaDomainMetadataUpdates updates = setDomainMetadata.get().getUpdates();
      return updates != null && updates.getDeltaClustering() != null;
    }

    /**
     * True if this request changes catalog-visible table metadata: any MANAGED-applicable metadata
     * action ({@link #hasManagedTableMetadataChange()}) or the EXTERNAL-only {@code
     * update-metadata-snapshot-version}. Data-only {@code add-commit} and backfill-only requests
     * carry no metadata change and return false.
     */
    public boolean changesTableMetadata() {
      return hasManagedTableMetadataChange() || updateSnapshotVersion.isPresent();
    }

    void putOnce(DeltaTableUpdate update) {
      if (update instanceof DeltaSetPropertiesUpdate u) {
        setProperties = fillOnce(setProperties, u, DeltaTableUpdate::getAction);
      } else if (update instanceof DeltaRemovePropertiesUpdate u) {
        removeProperties = fillOnce(removeProperties, u, DeltaTableUpdate::getAction);
      } else if (update instanceof DeltaSetProtocolUpdate u) {
        setProtocol = fillOnce(setProtocol, u, DeltaTableUpdate::getAction);
      } else if (update instanceof DeltaSetSchemaUpdate u) {
        setSchema = fillOnce(setSchema, u, DeltaTableUpdate::getAction);
      } else if (update instanceof DeltaSetPartitionColumnsUpdate u) {
        setPartitionColumns = fillOnce(setPartitionColumns, u, DeltaTableUpdate::getAction);
      } else if (update instanceof DeltaSetTableCommentUpdate u) {
        setTableComment = fillOnce(setTableComment, u, DeltaTableUpdate::getAction);
      } else if (update instanceof DeltaSetDomainMetadataUpdate u) {
        setDomainMetadata = fillOnce(setDomainMetadata, u, DeltaTableUpdate::getAction);
      } else if (update instanceof DeltaRemoveDomainMetadataUpdate u) {
        removeDomainMetadata = fillOnce(removeDomainMetadata, u, DeltaTableUpdate::getAction);
      } else if (update instanceof DeltaUpdateSnapshotVersionUpdate u) {
        updateSnapshotVersion = fillOnce(updateSnapshotVersion, u, DeltaTableUpdate::getAction);
      } else if (update instanceof DeltaAddCommitUpdate u) {
        addCommit = fillOnce(addCommit, u, DeltaTableUpdate::getAction);
      } else if (update instanceof DeltaSetLatestBackfilledVersionUpdate u) {
        setLatestBackfilledVersion =
            fillOnce(setLatestBackfilledVersion, u, DeltaTableUpdate::getAction);
      } else {
        throw new BaseException(
            ErrorCode.INVALID_ARGUMENT,
            "Unknown update action: " + (update == null ? "null" : update.getAction()));
      }
    }
  }

  private static CollectedRequirements collectRequirements(
      List<DeltaTableRequirement> requirements) {
    CollectedRequirements c = new CollectedRequirements();
    if (requirements != null) {
      requirements.forEach(c::putOnce);
    }
    return c;
  }

  private static CollectedUpdates collectUpdates(List<DeltaTableUpdate> updates) {
    CollectedUpdates c = new CollectedUpdates();
    updates.forEach(c::putOnce);
    return c;
  }

  /** One-shot slot fill; {@code nameFn} extracts the action/type name for the error message. */
  private static <T> Optional<T> fillOnce(Optional<T> slot, T value, Function<T, String> nameFn) {
    if (slot.isPresent()) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          "At most one " + nameFn.apply(value) + " is allowed per request.");
    }
    return Optional.of(value);
  }

  // ---------------------------------------------------------------------- requirements check

  /**
   * Checks the {@code assert-table-uuid} requirement, if present, raising {@link
   * ErrorCode#UPDATE_REQUIREMENT_CONFLICT} on mismatch.
   */
  public static void checkTableUuidRequirement(TableInfoDAO dao, CollectedRequest collected) {
    Optional<UUID> assertUuid =
        collected.requirements().assertTableUuid.map(DeltaAssertTableUUID::getUuid);
    UUID tableUuid = dao.getId();
    if (assertUuid.isPresent() && !Objects.equals(assertUuid.get(), tableUuid)) {
      throw new BaseException(
          ErrorCode.UPDATE_REQUIREMENT_CONFLICT,
          "assert-table-uuid failed: expected " + assertUuid.get() + " but table has " + tableUuid);
    }
  }

  /**
   * Checks the {@code assert-etag} requirement, if present, against {@code preApplyEtag}, raising
   * {@link ErrorCode#UPDATE_REQUIREMENT_CONFLICT} on mismatch. The caller passes the pre-apply etag
   * because applying a commit can advance {@code updatedAt} (and the etag), so recomputing here
   * would compare against post-mutation state.
   */
  public static void checkEtagRequirement(String preApplyEtag, CollectedRequest collected) {
    Optional<String> assertEtag = collected.requirements().assertEtag.map(DeltaAssertEtag::getEtag);
    if (assertEtag.isPresent() && !Objects.equals(preApplyEtag, assertEtag.get())) {
      throw new BaseException(
          ErrorCode.UPDATE_REQUIREMENT_CONFLICT,
          "assert-etag failed: expected " + assertEtag.get() + " but table has " + preApplyEtag);
    }
  }

  /**
   * Shared by the {@code loadTable} response builder and the {@code assert-etag} check.
   *
   * <p>Known weakness: ms-precision {@code updated_at}. If a state-changing update lands in the
   * same wall-clock millisecond as the client's prior read, the etag doesn't advance and a
   * follow-up {@code assert-etag} passes against state the client never observed. The Delta update
   * path holds {@code PESSIMISTIC_WRITE} on the row, so two concurrent {@code updateTableForDelta}
   * calls cannot both pass {@code assert-etag} against the same stale snapshot. {@code assert-etag}
   * is an optional client-side optimization anyway; the authoritative serialization for CCv2
   * commits is the version conflict check in the commit endpoint.
   */
  public static String computeEtag(TableInfoDAO dao) {
    return dao.getUpdatedAt() != null
        ? "etag-" + dao.getUpdatedAt().getTime()
        : "etag-" + dao.getId();
  }

  // ---------------------------------------------------------------------- apply updates

  /**
   * What the commit-log actions ({@code add-commit}, {@code set-latest-backfilled-version}) prepare
   * for the caller to dispatch via {@code DeltaCommitRepository.applyCommitAndBackfillInSession}.
   * Kept as a value type so the mapper stays free of the commit-repo dependency.
   */
  public record CommitDispatch(
      Optional<DeltaCommitInfo> commit,
      Optional<DeltaUniformUtils.UniformIcebergFields> uniformFields,
      Optional<Long> latestBackfilledVersion) {}

  /**
   * Apply the updates to the DAO and property map, returning the commit-log dispatch (if any) for
   * the caller to forward to {@code DeltaCommitRepository}.
   *
   * <p>Actions run in canonical order (not request order): schema + partition columns first (one
   * combined pass via {@link #applySchemaAndPartitionColumns} when both are present); then protocol
   * / properties / domain-metadata / comment / snapshot version so the UniForm-presence check sees
   * the post-update property map; finally the commit + backfill pair is shaped into a {@link
   * CommitDispatch}.
   */
  public static Optional<CommitDispatch> applyUpdates(
      Session session,
      TableInfoDAO dao,
      MutablePropertyMap properties,
      CollectedRequest collected,
      ServerProperties serverProperties) {
    CollectedUpdates c = collected.updates();
    String rawMode = properties.get(DeltaConsts.TableProperties.COLUMN_MAPPING_MODE);
    String columnMappingMode = rawMode != null ? rawMode.trim().toLowerCase(Locale.ROOT) : "none";
    applySchemaAndPartitionColumns(
        session, dao, c.setSchema, c.setPartitionColumns, columnMappingMode);
    c.setProtocol.ifPresent(u -> applySetProtocol(properties, u.getProtocol()));
    c.setProperties.ifPresent(u -> applySetProperties(properties, u.getUpdates()));
    c.removeProperties.ifPresent(u -> applyRemoveProperties(properties, u.getRemovals()));
    c.setDomainMetadata.ifPresent(u -> applySetDomainMetadata(properties, u.getUpdates()));
    c.removeDomainMetadata.ifPresent(u -> applyRemoveDomainMetadata(properties, u.getDomains()));
    c.setTableComment.ifPresent(u -> applySetTableComment(dao, u));
    c.updateSnapshotVersion.ifPresent(u -> applyUpdateSnapshotVersion(dao, properties, u));
    // Re-validate the MANAGED contract against the final post-apply state. set-protocol runs
    // full validation; set-domain-metadata alone only needs the DM-vs-writer-features check.
    if (TableType.MANAGED.toString().equals(dao.getType())) {
      DeltaDomainMetadataUpdates effectiveDm =
          DeltaPropertyMapper.synthesizeDomainMetadataFromProperties(properties.asMap());
      if (c.setProtocol.isPresent()) {
        UcManagedDeltaContract.validate(
            c.setProtocol.get().getProtocol(), effectiveDm, properties.asMap(), serverProperties);
      } else if (c.setDomainMetadata.isPresent()) {
        UcManagedDeltaContract.validateDomainMetadataAgainstWriterFeatures(
            DeltaPropertyMapper.extractFeaturesFromProperties(properties.asMap()), effectiveDm);
      }
    }
    if (c.addCommit.isPresent() || c.setLatestBackfilledVersion.isPresent()) {
      return Optional.of(
          prepareCommitAndBackfill(
              dao,
              properties,
              c.addCommit,
              c.setLatestBackfilledVersion,
              c.hasManagedTableMetadataChange()));
    }
    return Optional.empty();
  }

  private static void applySetProperties(MutablePropertyMap properties, Map<String, String> toSet) {
    if (toSet == null || toSet.isEmpty()) {
      return;
    }
    properties.putAll(toSet);
  }

  private static void applyRemoveProperties(MutablePropertyMap properties, List<String> toRemove) {
    if (toRemove == null || toRemove.isEmpty()) {
      return;
    }
    properties.removeAll(toRemove);
  }

  /** Full replacement of the protocol block; other stored properties are left alone. */
  private static void applySetProtocol(MutablePropertyMap properties, DeltaProtocol protocol) {
    ValidationUtils.checkNotNull(protocol, "set-protocol requires a protocol.");
    properties.removeMatchingPrefix(TableProperties.FEATURE_PREFIX);
    Map<String, String> derived = new HashMap<>();
    DeltaPropertyMapper.deriveFromProtocol(derived, protocol);
    properties.putAll(derived);
  }

  /**
   * Apply schema and/or partition-column changes by building the post-update column list once and
   * swapping the DAO column collection. The spec frames {@code set-columns} and {@code
   * set-partition-columns} as independent actions; the absent action's concern is preserved from
   * the existing DAO so a column-only request can't silently desync the partition list (and vice
   * versa). If a partition column is missing from the resulting schema, the request is rejected
   * with {@code partition-columns references unknown column: ...}.
   *
   * <p>Swap semantics rely on the orphanRemoval mapping on {@link TableInfoDAO#getColumns()} to
   * clean up the old rows; the intervening flush ensures the deletes hit the DB before the inserts,
   * so the {@code (table_id, ordinal_position, name)} unique constraint doesn't trip.
   *
   * <p>On every {@code set-columns} the DB-row UUID for each column is freshly minted (matching
   * managed-catalog behaviour). All hard-validation guards for column-mapping rename/drop/reassign
   * are enforced via {@link #validateColumnRenameAndDropGuards} before the swap runs.
   */
  private static void applySchemaAndPartitionColumns(
      Session session,
      TableInfoDAO dao,
      Optional<DeltaSetSchemaUpdate> setSchema,
      Optional<DeltaSetPartitionColumnsUpdate> setPartition,
      String columnMappingMode) {
    if (setSchema.isEmpty() && setPartition.isEmpty()) {
      return;
    }
    // Source of the new schema: the request when set-columns is present, otherwise the existing
    // DAO with partition indices cleared so applyPartitionColumns can re-stamp them below.
    List<ColumnInfo> newColumns;
    if (setSchema.isPresent()) {
      DeltaStructType columns =
          ValidationUtils.checkNotNull(
              setSchema.get().getColumns(), "set-columns requires a columns block.");
      List<DeltaStructField> fields =
          ValidationUtils.checkNotNull(columns.getFields(), "set-columns requires columns.fields.");
      if (fields.isEmpty()) {
        throw new BaseException(
            ErrorCode.INVALID_ARGUMENT, "set-columns requires at least one column.");
      }
      ColumnUtils.validateStructType(columns, "columns");
      newColumns = ColumnUtils.toColumnInfos(fields);
    } else {
      newColumns = ColumnInfoDAO.toList(dao.getColumns());
      newColumns.forEach(c -> c.setPartitionIndex(null));
    }
    // Source of the partition list: the request when set-partition-columns is present, otherwise
    // the existing DAO's partition columns (preserved by name across a column-only action).
    List<String> partitionNames;
    if (setPartition.isPresent()) {
      partitionNames =
          ValidationUtils.checkNotNull(
              setPartition.get().getPartitionColumns(),
              "set-partition-columns requires a partition-columns list.");
    } else {
      partitionNames = currentPartitionColumnNames(dao);
    }
    ColumnUtils.applyPartitionColumns(newColumns, partitionNames);
    // Hard-validation guards for column-mapping rename / drop / value-reassignment.
    // Must run before validateNoCmIdentityStripped (G5) since the guards short-circuit early on
    // no-CM and first-enable paths before G5 is reached.
    if (setSchema.isPresent()) {
      validateColumnRenameAndDropGuards(dao.getColumns(), newColumns, columnMappingMode);
      // G5: reject any incoming column that shares a logical name with an existing column but has
      // lost its column-mapping identity key (presence-only).
      validateNoCmIdentityStripped(dao.getColumns(), newColumns);
    }
    // Build the new DAO list. On every set-columns, every column receives a fresh UUID (matching
    // managed-catalog behaviour; the UUID has no known OSS consumers keying on it).
    List<ColumnInfoDAO> newColumnDAOs = ColumnInfoDAO.fromList(newColumns);
    newColumnDAOs.forEach(
        c -> {
          c.setId(UUID.randomUUID());
          c.setTable(dao);
        });
    dao.getColumns().clear();
    session.flush();
    dao.getColumns().addAll(newColumnDAOs);
  }

  // ---------------------------------------------------------------------- column-mapping guards

  /**
   * Enforces all hard-validation guards for column-mapping correctness on a {@code set-columns}
   * request. Guards G1-G8 mirror the DB UC (TS wrapper + managed-catalog) behaviour and are checked
   * in the same order as the internal stack:
   *
   * <ul>
   *   <li>G8 -- duplicate {@code delta.columnMapping.id} in the incoming schema (id-mode only,
   *       globally across all nesting levels).
   *   <li>G2 -- the incoming schema drops all column-mapping metadata while the current schema had
   *       it ("dropped column mapping metadata").
   *   <li>G1 -- no column mapping anywhere, but a column name was removed ("Column mapping is
   *       required to rename or drop a column").
   *   <li>G3 -- first-enable: an incoming column omits {@code delta.columnMapping.physicalName}
   *       ("physical column name").
   *   <li>G4 -- first-enable + rename in one request ("separate statement").
   *   <li>G5 -- handled separately via {@link #validateNoCmIdentityStripped}.
   *   <li>G6 -- more than one top-level rename in a single request ("single top-level column
   *       rename").
   *   <li>G7 -- same-name rename detected (new logical name case-insensitively equal to old) --
   *       defensive dead code with the current {@code equalsIgnoreCase} detection filter
   *       ("identical to the original").
   *   <li>Reassignment -- retained column's CM key value changed (id-mode: {@code
   *       delta.columnMapping.id}; name-mode: {@code delta.columnMapping.physicalName}).
   * </ul>
   */
  private static void validateColumnRenameAndDropGuards(
      List<ColumnInfoDAO> existingDaos,
      List<ColumnInfo> incomingColumns,
      String columnMappingMode) {
    List<ColumnInfo> existingColumns = ColumnInfoDAO.toList(existingDaos);

    // G8: duplicate delta.columnMapping.id in the incoming schema (id-mode only -- ids must be
    // globally unique across all nesting levels; physicalName duplicates are not checked here).
    detectDuplicateCmId(incomingColumns)
        .ifPresent(
            dupId -> {
              throw new BaseException(
                  ErrorCode.INVALID_ARGUMENT,
                  "Duplicate column mapping id '" + dupId + "' in incoming schema.");
            });

    boolean currentHasCm = hasAnyCmMetadata(existingColumns);
    boolean incomingHasCm = hasAnyCmMetadata(incomingColumns);

    if (!incomingHasCm) {
      if (currentHasCm) {
        // G2: incoming schema removed all column-mapping metadata that was present.
        throw new BaseException(
            ErrorCode.INVALID_ARGUMENT,
            "set-columns dropped column mapping metadata. "
                + "Removing column mapping is not supported via the Delta REST catalog.");
      }
      // G1: neither schema has column-mapping metadata; reject any name removal (which would be a
      // rename or drop without mapping, causing silent data loss in the underlying storage).
      List<String> removedNames = namesRemovedWithoutMapping(existingColumns, incomingColumns);
      if (!removedNames.isEmpty()) {
        throw new BaseException(
            ErrorCode.INVALID_ARGUMENT,
            "Column mapping is required to rename or drop a column; '"
                + removedNames.get(0)
                + "' cannot be removed while column mapping is disabled. "
                + "Enable column mapping first.");
      }
      // No CM anywhere, no names removed: pass through (add / reorder / type-change are safe).
      return;
    }

    if (!currentHasCm) {
      // First-enable path: the incoming schema introduces column-mapping metadata for the first
      // time (current has none).

      // G3: every top-level incoming column must carry physicalName so a hidden rename can be
      // safely ruled out (physicalName == current logical name on first-enable by convention).
      if (hasAnyMissingPhysicalName(incomingColumns)) {
        throw new BaseException(
            ErrorCode.INVALID_ARGUMENT,
            "set-columns: enabling column mapping requires a physical column name "
                + "for every top-level column; this request omits one, so a rename cannot be "
                + "safely ruled out.");
      }

      // G4: first-enable + rename in the same request is not supported. UC splits column-mapping
      // enable and rename into two separate commits; a single-commit rename would degrade to a
      // drop+add in managed-catalog.
      List<String> feRenames = detectFirstEnableRenames(existingColumns, incomingColumns);
      if (!feRenames.isEmpty()) {
        throw new BaseException(
            ErrorCode.INVALID_ARGUMENT,
            "set-columns: renaming a column while enabling column mapping is not supported "
                + "in a single request. Enable column mapping in a separate statement, "
                + "then rename the column.");
      }
      // Plain CM-enable with no rename: pass through.
      return;
    }

    // Normal path: the current schema already has column-mapping metadata.

    // G6: at most one top-level rename per request.
    List<TopLevelRename> topLevelRenames = detectTopLevelRenames(existingColumns, incomingColumns);
    if (topLevelRenames.size() > 1) {
      String renameList =
          topLevelRenames.stream()
              .map(r -> r.oldLogicalName() + "->" + r.newLogicalName())
              .sorted()
              .collect(Collectors.joining(", "));
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          "Only a single top-level column rename is supported per request; this request renames "
              + topLevelRenames.size()
              + " columns ["
              + renameList
              + "]. Split them into separate ALTER statements.");
    }

    // G7: defensive guard -- a "rename" where the new name is case-insensitively equal to the old
    // name is a same-name rename (identical to the original). With the equalsIgnoreCase filter
    // inside detectTopLevelRenames this branch is dead code in practice; it remains as a safety
    // net in case detection logic changes.
    for (TopLevelRename rename : topLevelRenames) {
      if (rename.newLogicalName().equalsIgnoreCase(rename.oldLogicalName())) {
        throw new BaseException(
            ErrorCode.INVALID_ARGUMENT,
            "Column rename to '"
                + rename.newLogicalName()
                + "' is identical to the original column name.");
      }
    }

    // Reassignment guard: a retained column (same logical name in both schemas) must not change
    // its column-mapping key value. The mode (id vs. name) determines which key to check.
    // columnMappingMode is passed in from the table property (delta.columnMapping.mode).
    if (!"none".equals(columnMappingMode)) {
      List<String> reassigned =
          detectCmValueReassignment(existingColumns, incomingColumns, columnMappingMode);
      if (!reassigned.isEmpty()) {
        reassigned = new ArrayList<>(reassigned);
        reassigned.sort(String::compareTo);
        String keyName = "id".equals(columnMappingMode) ? "id" : "physicalName";
        throw new BaseException(
            ErrorCode.INVALID_ARGUMENT,
            "Column mapping "
                + keyName
                + " reassignment is not supported for retained column(s): "
                + String.join(", ", reassigned));
      }
    }
  }

  /**
   * Returns {@code true} if any column in {@code columns} carries a column-mapping identity key
   * ({@code delta.columnMapping.id} or {@code delta.columnMapping.physicalName}) in its {@code
   * type_json} metadata.
   */
  private static boolean hasAnyCmMetadata(List<ColumnInfo> columns) {
    return columns.stream().anyMatch(c -> mappingKeyOf(c) != null);
  }

  /**
   * Returns the logical names of top-level columns present in {@code current} but absent (by
   * case-insensitive name comparison) from {@code incoming}. A non-empty result signals that a
   * column was renamed or dropped without column-mapping metadata to track the identity change --
   * which would cause silent data loss in the underlying storage engine.
   */
  private static List<String> namesRemovedWithoutMapping(
      List<ColumnInfo> current, List<ColumnInfo> incoming) {
    Set<String> incomingByLower =
        incoming.stream()
            .map(c -> c.getName().toLowerCase(Locale.ROOT))
            .collect(Collectors.toSet());
    List<String> removed = new ArrayList<>();
    for (ColumnInfo cur : current) {
      if (!incomingByLower.contains(cur.getName().toLowerCase(Locale.ROOT))) {
        removed.add(cur.getName());
      }
    }
    return removed;
  }

  /**
   * Returns {@code true} if any top-level incoming column lacks a {@code
   * delta.columnMapping.physicalName} metadata entry. Used by the G3 guard to detect a first-enable
   * request where a hidden rename cannot be safely ruled out.
   */
  private static boolean hasAnyMissingPhysicalName(List<ColumnInfo> columns) {
    for (ColumnInfo c : columns) {
      DeltaStructField field = ColumnUtils.toStructField(c);
      DeltaStructFieldMetadata meta = field.getMetadata();
      if (meta == null || meta.get("delta.columnMapping.physicalName") == null) {
        return true;
      }
    }
    return false;
  }

  /**
   * Detects column renames on the first-enable path: incoming physicalName matches a current
   * column's logical name (by Delta convention physicalName == current logical name on
   * first-enable) but the incoming logical name differs, indicating a hidden rename.
   *
   * @return human-readable rename descriptions, e.g. {@code "oldName -> newName"}
   */
  private static List<String> detectFirstEnableRenames(
      List<ColumnInfo> currentColumns, List<ColumnInfo> incomingColumns) {
    Map<String, String> currentByLower =
        currentColumns.stream()
            .collect(
                Collectors.toMap(c -> c.getName().toLowerCase(Locale.ROOT), ColumnInfo::getName));
    List<String> renames = new ArrayList<>();
    for (ColumnInfo inc : incomingColumns) {
      DeltaStructField field = ColumnUtils.toStructField(inc);
      DeltaStructFieldMetadata meta = field.getMetadata();
      if (meta == null) continue;
      Object physObj = meta.get("delta.columnMapping.physicalName");
      if (!(physObj instanceof String physName)) continue;
      // By Delta convention on first-enable, physicalName == current logical name; a mismatch
      // between the incoming logical name and the current column whose logical name equals the
      // physicalName is a rename.
      String currentLogical = currentByLower.get(physName.toLowerCase(Locale.ROOT));
      if (currentLogical != null && !inc.getName().equalsIgnoreCase(currentLogical)) {
        renames.add(currentLogical + " -> " + inc.getName());
      }
    }
    return renames;
  }

  /**
   * Represents a detected top-level column rename: the column's CM identity key stayed the same
   * (physicalName in name-mode, {@code delta.columnMapping.id} in id-mode) while its logical name
   * changed.
   */
  private record TopLevelRename(String oldLogicalName, String newLogicalName) {}

  /**
   * Detects top-level column renames by matching current and incoming columns on their CM identity
   * key (per-column: prefers {@code delta.columnMapping.id}; falls back to {@code
   * delta.columnMapping.physicalName}). Only top-level columns are compared; nested struct-field
   * renames ride inside the column's {@code type_json} and are not surfaced here.
   *
   * <p>Case-insensitive name comparison: a change from "A" to "a" is not considered a rename
   * (matching the DB UC TS-wrapper's {@code equalsIgnoreCase} filter). This is the reason G7 cannot
   * be triggered in the current implementation.
   */
  private static List<TopLevelRename> detectTopLevelRenames(
      List<ColumnInfo> currentColumns, List<ColumnInfo> incomingColumns) {
    Map<String, String> currentByKey = new HashMap<>();
    for (ColumnInfo c : currentColumns) {
      String key = mappingKeyOf(c);
      if (key != null) currentByKey.put(key, c.getName());
    }
    Map<String, String> incomingByKey = new HashMap<>();
    for (ColumnInfo c : incomingColumns) {
      String key = mappingKeyOf(c);
      if (key != null) incomingByKey.put(key, c.getName());
    }
    List<TopLevelRename> renames = new ArrayList<>();
    for (Map.Entry<String, String> entry : currentByKey.entrySet()) {
      String oldName = entry.getValue();
      String newName = incomingByKey.get(entry.getKey());
      // equalsIgnoreCase: case-only changes ("A"→"a") are not renames.
      if (newName != null && !newName.equalsIgnoreCase(oldName)) {
        renames.add(new TopLevelRename(oldName, newName));
      }
    }
    return renames;
  }

  /**
   * Detects duplicate {@code delta.columnMapping.id} values in the incoming schema, recursing into
   * nested struct, array, and map types. Column IDs must be globally unique across all nesting
   * levels in id-mode.
   *
   * @return the first duplicate id found, or empty if all ids are unique
   */
  private static Optional<String> detectDuplicateCmId(List<ColumnInfo> incomingColumns) {
    Set<String> seen = new HashSet<>();
    for (ColumnInfo c : incomingColumns) {
      DeltaStructField field = ColumnUtils.toStructField(c);
      Optional<String> dup = collectCmIds(field, seen);
      if (dup.isPresent()) return dup;
    }
    return Optional.empty();
  }

  /** Checks {@code field}'s CM id and recurses into its type for further nested ids. */
  private static Optional<String> collectCmIds(DeltaStructField field, Set<String> seen) {
    DeltaStructFieldMetadata meta = field.getMetadata();
    if (meta != null) {
      Object idObj = meta.get("delta.columnMapping.id");
      if (idObj != null) {
        String id = idObj.toString();
        if (!seen.add(id)) {
          return Optional.of(id);
        }
      }
    }
    return collectCmIdsInType(field.getType(), seen);
  }

  /** Recurses into complex types (struct / array / map) to collect CM ids. */
  private static Optional<String> collectCmIdsInType(DeltaDataType type, Set<String> seen) {
    if (type instanceof DeltaStructType struct) {
      List<DeltaStructField> fields = struct.getFields();
      if (fields != null) {
        for (DeltaStructField f : fields) {
          Optional<String> dup = collectCmIds(f, seen);
          if (dup.isPresent()) return dup;
        }
      }
    } else if (type instanceof DeltaArrayType array) {
      return collectCmIdsInType(array.getElementType(), seen);
    } else if (type instanceof DeltaMapType map) {
      Optional<String> dup = collectCmIdsInType(map.getKeyType(), seen);
      if (dup.isPresent()) return dup;
      return collectCmIdsInType(map.getValueType(), seen);
    }
    return Optional.empty();
  }

  /**
   * Detects column-mapping key value reassignment on retained columns (columns with the same
   * logical name in both the current and incoming schemas). In id-mode, the {@code
   * delta.columnMapping.id} value must not change; in name-mode, the {@code
   * delta.columnMapping.physicalName} value must not change. Recurses into nested struct fields.
   *
   * @return sorted list of logical names of columns whose CM key value was reassigned
   */
  private static List<String> detectCmValueReassignment(
      List<ColumnInfo> currentColumns, List<ColumnInfo> incomingColumns, String mode) {
    Map<String, ColumnInfo> currentByLower =
        currentColumns.stream()
            .collect(
                Collectors.toMap(c -> c.getName().toLowerCase(Locale.ROOT), Function.identity()));
    List<String> violations = new ArrayList<>();
    for (ColumnInfo inc : incomingColumns) {
      ColumnInfo cur = currentByLower.get(inc.getName().toLowerCase(Locale.ROOT));
      if (cur != null) {
        DeltaStructField curField = ColumnUtils.toStructField(cur);
        DeltaStructField incField = ColumnUtils.toStructField(inc);
        violations.addAll(checkCmValueConsistency(curField, incField, mode, inc.getName()));
      }
    }
    return violations;
  }

  /**
   * Checks that the CM key value of {@code incField} matches {@code curField} for the given {@code
   * mode}, and recurses into any nested struct / array / map types. Returns the logical names of
   * fields (at the path described by {@code fieldLabel}) whose CM key value changed.
   */
  private static List<String> checkCmValueConsistency(
      DeltaStructField curField, DeltaStructField incField, String mode, String fieldLabel) {
    List<String> violations = new ArrayList<>();
    String cmKey =
        "id".equals(mode) ? "delta.columnMapping.id" : "delta.columnMapping.physicalName";
    String curValue = getCmKeyValue(curField, cmKey);
    String incValue = getCmKeyValue(incField, cmKey);
    // Only flag when BOTH schemas carry the key (presence-only strip is handled by G5).
    if (curValue != null && incValue != null && !curValue.equals(incValue)) {
      violations.add(fieldLabel);
    }
    // Recurse into nested struct types via the type trees.
    violations.addAll(
        checkCmValueConsistencyInTypes(curField.getType(), incField.getType(), mode, fieldLabel));
    return violations;
  }

  /** Recurses into matching complex-type pairs to check CM value consistency of nested fields. */
  private static List<String> checkCmValueConsistencyInTypes(
      DeltaDataType curType, DeltaDataType incType, String mode, String parentLabel) {
    List<String> violations = new ArrayList<>();
    if (curType instanceof DeltaStructType curStruct
        && incType instanceof DeltaStructType incStruct) {
      List<DeltaStructField> curFields =
          curStruct.getFields() != null ? curStruct.getFields() : List.of();
      // Build a map of current nested fields by lower-case logical name.
      Map<String, DeltaStructField> curByLower =
          curFields.stream()
              .collect(
                  Collectors.toMap(f -> f.getName().toLowerCase(Locale.ROOT), Function.identity()));
      List<DeltaStructField> incFields =
          incStruct.getFields() != null ? incStruct.getFields() : List.of();
      for (DeltaStructField incF : incFields) {
        DeltaStructField curF = curByLower.get(incF.getName().toLowerCase(Locale.ROOT));
        if (curF != null) {
          violations.addAll(
              checkCmValueConsistency(curF, incF, mode, parentLabel + "." + incF.getName()));
        }
      }
    } else if (curType instanceof DeltaArrayType curArr
        && incType instanceof DeltaArrayType incArr) {
      violations.addAll(
          checkCmValueConsistencyInTypes(
              curArr.getElementType(), incArr.getElementType(), mode, parentLabel));
    } else if (curType instanceof DeltaMapType curMap && incType instanceof DeltaMapType incMap) {
      violations.addAll(
          checkCmValueConsistencyInTypes(
              curMap.getKeyType(), incMap.getKeyType(), mode, parentLabel));
      violations.addAll(
          checkCmValueConsistencyInTypes(
              curMap.getValueType(), incMap.getValueType(), mode, parentLabel));
    }
    return violations;
  }

  /**
   * Returns the string value of {@code cmKey} from {@code field}'s metadata, or {@code null} if the
   * key is absent or its value is null.
   */
  private static String getCmKeyValue(DeltaStructField field, String cmKey) {
    DeltaStructFieldMetadata meta = field.getMetadata();
    if (meta == null) return null;
    Object val = meta.get(cmKey);
    return val != null ? val.toString() : null;
  }

  /**
   * Returns the column-mapping identity key for a DAO column. Prefers {@code
   * delta.columnMapping.id} (id-mode), falls back to {@code delta.columnMapping.physicalName}
   * (name-mode), and returns {@code null} if neither is present (no column mapping). The returned
   * key includes a namespace prefix ({@code "id:"} or {@code "phys:"}) to prevent a numeric
   * physical-name value from colliding with a real CM id.
   */
  private static String mappingKeyOf(ColumnInfoDAO dao) {
    return mappingKeyOf(ColumnUtils.toStructField(dao.toColumnInfo()));
  }

  /**
   * Returns the column-mapping identity key for a {@link ColumnInfo}. See {@link
   * #mappingKeyOf(ColumnInfoDAO)}.
   */
  private static String mappingKeyOf(ColumnInfo column) {
    return mappingKeyOf(ColumnUtils.toStructField(column));
  }

  /** Shared CM identity key extractor for a {@link DeltaStructField}. */
  private static String mappingKeyOf(DeltaStructField field) {
    DeltaStructFieldMetadata meta = field.getMetadata();
    if (meta == null) {
      return null;
    }
    Object id = meta.get("delta.columnMapping.id");
    if (id != null) {
      return "id:" + id;
    }
    Object physName = meta.get("delta.columnMapping.physicalName");
    if (physName instanceof String s) {
      return "phys:" + s;
    }
    return null;
  }

  /**
   * Rejects a {@code set-columns} where a column present in both the existing schema and the
   * incoming schema (matched by logical name) has lost its column-mapping identity key. This is a
   * presence-only check: value reassignment ({@code id} or {@code physicalName} changed while still
   * present) is validated separately by the reassignment guard in {@link
   * #validateColumnRenameAndDropGuards}.
   *
   * <p>Columns with no CM identity in the existing schema are not subject to this check -- they can
   * safely transition to or from CM metadata.
   *
   * @throws BaseException with {@link ErrorCode#INVALID_ARGUMENT} naming the offending columns
   */
  private static void validateNoCmIdentityStripped(
      List<ColumnInfoDAO> existingDaos, List<ColumnInfo> incomingColumns) {
    // Build a map from logical name to CM key for existing columns that have CM metadata.
    Map<String, String> existingCmKeyByName = new HashMap<>();
    for (ColumnInfoDAO dao : existingDaos) {
      String key = mappingKeyOf(dao);
      if (key != null) {
        existingCmKeyByName.put(dao.getName(), key);
      }
    }
    // Find columns that are retained (same logical name) but lost their CM identity key.
    List<String> stripped = new ArrayList<>();
    for (ColumnInfo col : incomingColumns) {
      if (existingCmKeyByName.containsKey(col.getName()) && mappingKeyOf(col) == null) {
        stripped.add(col.getName());
      }
    }
    if (!stripped.isEmpty()) {
      stripped.sort(String::compareTo);
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          "set-columns dropped column mapping metadata for column(s): "
              + String.join(", ", stripped));
    }
  }

  /** Names of the DAO's partition columns, ordered by partition index. */
  private static List<String> currentPartitionColumnNames(TableInfoDAO dao) {
    return ColumnInfoDAO.toList(dao.getColumns()).stream()
        .filter(c -> c.getPartitionIndex() != null)
        .sorted(Comparator.comparingInt(ColumnInfo::getPartitionIndex))
        .map(ColumnInfo::getName)
        .collect(Collectors.toList());
  }

  private static void applySetDomainMetadata(
      MutablePropertyMap properties, DeltaDomainMetadataUpdates updates) {
    ValidationUtils.checkNotNull(updates, "set-domain-metadata requires an updates block.");
    Map<String, String> derived = new HashMap<>();
    DeltaPropertyMapper.deriveFromDomainMetadata(derived, updates);
    if (derived.isEmpty()) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          "set-domain-metadata requires at least one domain entry. "
              + "Supported domains: "
              + new TreeSet<>(DeltaPropertyMapper.DOMAIN_TO_PROPERTY_KEY.keySet()));
    }
    properties.putAll(derived);
  }

  private static void applyRemoveDomainMetadata(
      MutablePropertyMap properties, List<String> domains) {
    if (domains == null || domains.isEmpty()) {
      return;
    }
    for (String domain : domains) {
      String propertyKey = DeltaPropertyMapper.DOMAIN_TO_PROPERTY_KEY.get(domain);
      if (propertyKey == null) {
        throw new BaseException(
            ErrorCode.INVALID_ARGUMENT, "Unknown domain in remove-domain-metadata: " + domain);
      }
      properties.remove(propertyKey);
    }
  }

  private static void applySetTableComment(TableInfoDAO dao, DeltaSetTableCommentUpdate update) {
    ValidationUtils.checkNotNull(update.getComment(), "set-table-comment requires a comment.");
    dao.setComment(update.getComment());
  }

  private static void applyUpdateSnapshotVersion(
      TableInfoDAO dao, MutablePropertyMap properties, DeltaUpdateSnapshotVersionUpdate update) {
    if (!TableType.EXTERNAL.toString().equals(dao.getType())) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          "update-metadata-snapshot-version is only supported for EXTERNAL Delta tables; "
              + "for MANAGED tables, use the Delta commit endpoint.");
    }
    ValidationUtils.checkNotNull(
        update.getLastCommitVersion(),
        "update-metadata-snapshot-version requires last-commit-version.");
    ValidationUtils.checkNotNull(
        update.getLastCommitTimestampMs(),
        "update-metadata-snapshot-version requires last-commit-timestamp-ms.");
    properties.put(
        TableProperties.LAST_UPDATE_VERSION, String.valueOf(update.getLastCommitVersion()));
    properties.put(
        TableProperties.LAST_COMMIT_TIMESTAMP, String.valueOf(update.getLastCommitTimestampMs()));
  }

  // ---------------------------------------------------------------------- commit + backfill

  /**
   * Mapper-side preparation for the {@code add-commit} and {@code set-latest-backfilled-version}
   * actions: cross-action validation plus snapshot-property bookkeeping. Returns the {@link
   * CommitDispatch} the caller forwards to {@code
   * DeltaCommitRepository.applyCommitAndBackfillInSession} so the mapper stays free of the
   * commit-repo dependency.
   *
   * <ul>
   *   <li>require MANAGED -- both actions are only legal on UC catalog-managed Delta tables (DELTA
   *       format is guaranteed by the caller's prior {@code requireDeltaTable});
   *   <li>extract and shape-validate uniform fields when {@code add-commit} carries them;
   *   <li>pin uniform.iceberg.converted-delta-version equal to commit.version (the commit-time
   *       check that the create-time uniform validator can't run because there is no commit version
   *       at create);
   *   <li>run the UniForm presence-consistency check against the post-update property map;
   *   <li>stamp {@code delta.lastUpdateVersion} / {@code delta.lastCommitTimestamp} when the commit
   *       is metadata-changing;
   *   <li>read {@code latest-published-version} off the backfill action and reject if it's null.
   * </ul>
   */
  private static CommitDispatch prepareCommitAndBackfill(
      TableInfoDAO dao,
      MutablePropertyMap properties,
      Optional<DeltaAddCommitUpdate> addCommitOpt,
      Optional<DeltaSetLatestBackfilledVersionUpdate> backfillOpt,
      boolean hasManagedTableMetadataChange) {
    requireManaged(dao);
    Optional<DeltaUniformUtils.UniformIcebergFields> uniformFields = Optional.empty();
    Optional<DeltaCommit> commitOpt = Optional.empty();
    if (addCommitOpt.isPresent()) {
      DeltaAddCommitUpdate addCommit = addCommitOpt.get();
      DeltaCommit commit =
          ValidationUtils.checkNotNull(
              addCommit.getCommit(), "add-commit requires a commit block.");
      commitOpt = Optional.of(commit);
      uniformFields = DeltaUniformUtils.getUniformFields(addCommit.getUniform());
      uniformFields.ifPresent(
          f -> DeltaUniformUtils.requireConvertedDeltaVersionEquals(f, commit.getVersion()));
      DeltaUniformUtils.validateConsistency(properties.asMap(), uniformFields.isPresent());
      // For MANAGED tables, a metadata-changing add-commit advances the snapshot bookkeeping
      // properties. UC is the authoritative commit coordinator, so the client doesn't need to
      // send them separately.
      if (hasManagedTableMetadataChange) {
        properties.put(TableProperties.LAST_UPDATE_VERSION, String.valueOf(commit.getVersion()));
        properties.put(
            TableProperties.LAST_COMMIT_TIMESTAMP, String.valueOf(commit.getTimestamp()));
      }
    }
    // The Delta wire field is named `latest-published-version`; the UC repo's parameter is
    // `latestBackfilledVersion` (same value, different perspective). Keep the local aligned with
    // the repo so the call site reads cleanly.
    Optional<Long> latestBackfilledVersion =
        backfillOpt.map(
            b ->
                ValidationUtils.checkNotNull(
                    b.getLatestPublishedVersion(),
                    "set-latest-backfilled-version requires latest-published-version."));
    return new CommitDispatch(
        commitOpt.map(DeltaUpdateTableMapper::toUcCommitInfo),
        uniformFields,
        latestBackfilledVersion);
  }

  /**
   * Convert a {@link DeltaCommit} into the UC {@link DeltaCommitInfo} shape so the Delta update
   * path can flow through the shared commit-log helpers, which speak the UC wire shape.
   */
  private static DeltaCommitInfo toUcCommitInfo(DeltaCommit commit) {
    return new DeltaCommitInfo()
        .version(commit.getVersion())
        .timestamp(commit.getTimestamp())
        .fileName(commit.getFileName())
        .fileSize(commit.getFileSize())
        .fileModificationTimestamp(commit.getFileModificationTimestamp());
  }

  /**
   * Reject non-MANAGED tables at the commit/backfill entry. DELTA format is guaranteed by the
   * caller's prior {@code requireDeltaTable}, so we only check the type here.
   */
  private static void requireManaged(TableInfoDAO dao) {
    if (!TableType.MANAGED.toString().equals(dao.getType())) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          "add-commit and set-latest-backfilled-version require a MANAGED Delta table.");
    }
  }
}
