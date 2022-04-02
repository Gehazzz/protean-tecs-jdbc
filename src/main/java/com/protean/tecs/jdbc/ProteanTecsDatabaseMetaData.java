package com.protean.tecs.jdbc;

import com.protean.tecs.jdbc.client.AnalysisServiceClient;
import com.protean.tecs.jdbc.dto.ColumnsToHide;
import com.protean.tecs.jdbc.dto.ResultSetData;
import com.protean.tecs.jdbc.dto.TablesToHide;
import com.protean.tecs.jdbc.operation.ResultSetMapping;
import com.protean.tecs.jdbc.operation.ResultSetMappingToResultSetData;
import com.protean.tecs.jdbc.operation.ResultSetModification;
import com.protean.tecs.jdbc.operation.ResultSetModificationImpl;
import com.sun.rowset.CachedRowSetImpl;
import lombok.SneakyThrows;

import javax.sql.rowset.CachedRowSet;
import java.sql.*;
import java.util.Arrays;

public class ProteanTecsDatabaseMetaData implements DatabaseMetaData {

    private final DatabaseMetaData databaseMetaData;
    private final String db;
    private final ProteanTecsJdbcConnection jdbcConnection;
    private final AnalysisServiceClient client;
    private final ResultSetModification resultSetModification;
    private final ResultSetMapping<ResultSetData> resultSetMapping;

    public ProteanTecsDatabaseMetaData(DatabaseMetaData databaseMetaData, String db, ProteanTecsJdbcConnection jdbcConnection, AnalysisServiceClient client) {
        this.databaseMetaData = databaseMetaData;
        this.db = db;
        this.jdbcConnection = jdbcConnection;
        this.client = client;
        this.resultSetModification = new ResultSetModificationImpl();
        this.resultSetMapping = new ResultSetMappingToResultSetData();
    }

    @Override
    public boolean allProceduresAreCallable() throws SQLException {
        return databaseMetaData.allProceduresAreCallable();
    }

    @Override
    public boolean allTablesAreSelectable() throws SQLException {
        return databaseMetaData.allTablesAreSelectable();
    }

    @Override
    public String getURL() throws SQLException {
        return databaseMetaData.getURL();
    }

    @Override
    public String getUserName() throws SQLException {
        return databaseMetaData.getUserName();
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return databaseMetaData.isReadOnly();
    }

    @Override
    public boolean nullsAreSortedHigh() throws SQLException {
        return databaseMetaData.nullsAreSortedHigh();
    }

    @Override
    public boolean nullsAreSortedLow() throws SQLException {
        return databaseMetaData.nullsAreSortedLow();
    }

    @Override
    public boolean nullsAreSortedAtStart() throws SQLException {
        return databaseMetaData.nullsAreSortedAtStart();
    }

    @Override
    public boolean nullsAreSortedAtEnd() throws SQLException {
        return databaseMetaData.nullsAreSortedAtEnd();
    }

    @Override
    public String getDatabaseProductName() throws SQLException {
        return databaseMetaData.getDatabaseProductName();
    }

    @Override
    public String getDatabaseProductVersion() throws SQLException {
        return databaseMetaData.getDatabaseProductVersion();
    }

    @Override
    public String getDriverName() throws SQLException {
        return databaseMetaData.getDriverName();
    }

    @Override
    public String getDriverVersion() throws SQLException {
        return databaseMetaData.getDriverVersion();
    }

    @Override
    public int getDriverMajorVersion() {
        return databaseMetaData.getDriverMajorVersion();
    }

    @Override
    public int getDriverMinorVersion() {
        return databaseMetaData.getDriverMinorVersion();
    }

    @Override
    public boolean usesLocalFiles() throws SQLException {
        return databaseMetaData.usesLocalFiles();
    }

    @Override
    public boolean usesLocalFilePerTable() throws SQLException {
        return databaseMetaData.usesLocalFilePerTable();
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        return databaseMetaData.supportsMixedCaseIdentifiers();
    }

    @Override
    public boolean storesUpperCaseIdentifiers() throws SQLException {
        return databaseMetaData.storesUpperCaseIdentifiers();
    }

    @Override
    public boolean storesLowerCaseIdentifiers() throws SQLException {
        return databaseMetaData.storesLowerCaseIdentifiers();
    }

    @Override
    public boolean storesMixedCaseIdentifiers() throws SQLException {
        return databaseMetaData.storesMixedCaseIdentifiers();
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        return databaseMetaData.supportsMixedCaseQuotedIdentifiers();
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        return databaseMetaData.storesUpperCaseQuotedIdentifiers();
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        return databaseMetaData.storesLowerCaseQuotedIdentifiers();
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        return databaseMetaData.storesMixedCaseQuotedIdentifiers();
    }

    @Override
    public String getIdentifierQuoteString() throws SQLException {
        String identifierQuoteString = databaseMetaData.getIdentifierQuoteString();
        client.sendQuery(identifierQuoteString);
        return identifierQuoteString;
    }

    @Override
    public String getSQLKeywords() throws SQLException {
        String sqlKeywords = databaseMetaData.getSQLKeywords();
        client.sendQuery(sqlKeywords);
        return sqlKeywords;
    }

    @Override
    public String getNumericFunctions() throws SQLException {
        return databaseMetaData.getNumericFunctions();
    }

    @Override
    public String getStringFunctions() throws SQLException {
        return databaseMetaData.getStringFunctions();
    }

    @Override
    public String getSystemFunctions() throws SQLException {
        return databaseMetaData.getSystemFunctions();
    }

    @Override
    public String getTimeDateFunctions() throws SQLException {
        return databaseMetaData.getTimeDateFunctions();
    }

    @Override
    public String getSearchStringEscape() throws SQLException {
        return databaseMetaData.getSearchStringEscape();
    }

    @Override
    public String getExtraNameCharacters() throws SQLException {
        return databaseMetaData.getExtraNameCharacters();
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        return databaseMetaData.supportsAlterTableWithAddColumn();
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        return databaseMetaData.supportsAlterTableWithDropColumn();
    }

    @Override
    public boolean supportsColumnAliasing() throws SQLException {
        return databaseMetaData.supportsColumnAliasing();
    }

    @Override
    public boolean nullPlusNonNullIsNull() throws SQLException {
        return databaseMetaData.nullPlusNonNullIsNull();
    }

    @Override
    public boolean supportsConvert() throws SQLException {
        return databaseMetaData.supportsConvert();
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) throws SQLException {
        return databaseMetaData.supportsConvert(fromType, toType);
    }

    @Override
    public boolean supportsTableCorrelationNames() throws SQLException {
        return databaseMetaData.supportsTableCorrelationNames();
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        return databaseMetaData.supportsDifferentTableCorrelationNames();
    }

    @Override
    public boolean supportsExpressionsInOrderBy() throws SQLException {
        return databaseMetaData.supportsExpressionsInOrderBy();
    }

    @Override
    public boolean supportsOrderByUnrelated() throws SQLException {
        return databaseMetaData.supportsOrderByUnrelated();
    }

    @Override
    public boolean supportsGroupBy() throws SQLException {
        return databaseMetaData.supportsGroupBy();
    }

    @Override
    public boolean supportsGroupByUnrelated() throws SQLException {
        return databaseMetaData.supportsGroupByUnrelated();
    }

    @Override
    public boolean supportsGroupByBeyondSelect() throws SQLException {
        return databaseMetaData.supportsGroupByBeyondSelect();
    }

    @Override
    public boolean supportsLikeEscapeClause() throws SQLException {
        return databaseMetaData.supportsLikeEscapeClause();
    }

    @Override
    public boolean supportsMultipleResultSets() throws SQLException {
        return databaseMetaData.supportsMultipleResultSets();
    }

    @Override
    public boolean supportsMultipleTransactions() throws SQLException {
        return databaseMetaData.supportsMultipleTransactions();
    }

    @Override
    public boolean supportsNonNullableColumns() throws SQLException {
        return databaseMetaData.supportsNonNullableColumns();
    }

    @Override
    public boolean supportsMinimumSQLGrammar() throws SQLException {
        return databaseMetaData.supportsMinimumSQLGrammar();
    }

    @Override
    public boolean supportsCoreSQLGrammar() throws SQLException {
        return databaseMetaData.supportsCoreSQLGrammar();
    }

    @Override
    public boolean supportsExtendedSQLGrammar() throws SQLException {
        return databaseMetaData.supportsExtendedSQLGrammar();
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        return databaseMetaData.supportsANSI92EntryLevelSQL();
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        return databaseMetaData.supportsANSI92IntermediateSQL();
    }

    @Override
    public boolean supportsANSI92FullSQL() throws SQLException {
        return databaseMetaData.supportsANSI92FullSQL();
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        return databaseMetaData.supportsIntegrityEnhancementFacility();
    }

    @Override
    public boolean supportsOuterJoins() throws SQLException {
        return databaseMetaData.supportsOuterJoins();
    }

    @Override
    public boolean supportsFullOuterJoins() throws SQLException {
        return databaseMetaData.supportsFullOuterJoins();
    }

    @Override
    public boolean supportsLimitedOuterJoins() throws SQLException {
        return databaseMetaData.supportsLimitedOuterJoins();
    }

    @Override
    public String getSchemaTerm() throws SQLException {
        String schemaTerm = databaseMetaData.getSchemaTerm();
        client.sendQuery(schemaTerm);
        return schemaTerm;
    }

    @Override
    public String getProcedureTerm() throws SQLException {
        String procedureTerm = databaseMetaData.getProcedureTerm();
        client.sendQuery(procedureTerm);
        return procedureTerm;
    }

    @Override
    public String getCatalogTerm() throws SQLException {
        String catalogTerm = databaseMetaData.getCatalogTerm();
        client.sendQuery(catalogTerm);
        return catalogTerm;
    }

    @Override
    public boolean isCatalogAtStart() throws SQLException {
        return databaseMetaData.isCatalogAtStart();
    }

    @Override
    public String getCatalogSeparator() throws SQLException {
        return databaseMetaData.getCatalogSeparator();
    }

    @Override
    public boolean supportsSchemasInDataManipulation() throws SQLException {
        return databaseMetaData.supportsSchemasInDataManipulation();
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        return databaseMetaData.supportsSchemasInProcedureCalls();
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        return databaseMetaData.supportsSchemasInTableDefinitions();
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        return databaseMetaData.supportsSchemasInIndexDefinitions();
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        return databaseMetaData.supportsSchemasInPrivilegeDefinitions();
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        return databaseMetaData.supportsCatalogsInDataManipulation();
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        return databaseMetaData.supportsCatalogsInProcedureCalls();
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        return databaseMetaData.supportsCatalogsInTableDefinitions();
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        return databaseMetaData.supportsCatalogsInIndexDefinitions();
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        return databaseMetaData.supportsCatalogsInPrivilegeDefinitions();
    }

    @Override
    public boolean supportsPositionedDelete() throws SQLException {
        return databaseMetaData.supportsPositionedDelete();
    }

    @Override
    public boolean supportsPositionedUpdate() throws SQLException {
        return databaseMetaData.supportsPositionedUpdate();
    }

    @Override
    public boolean supportsSelectForUpdate() throws SQLException {
        return databaseMetaData.supportsSelectForUpdate();
    }

    @Override
    public boolean supportsStoredProcedures() throws SQLException {
        return databaseMetaData.supportsStoredProcedures();
    }

    @Override
    public boolean supportsSubqueriesInComparisons() throws SQLException {
        return databaseMetaData.supportsSubqueriesInComparisons();
    }

    @Override
    public boolean supportsSubqueriesInExists() throws SQLException {
        return databaseMetaData.supportsSubqueriesInExists();
    }

    @Override
    public boolean supportsSubqueriesInIns() throws SQLException {
        return databaseMetaData.supportsSubqueriesInIns();
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        return databaseMetaData.supportsSubqueriesInQuantifieds();
    }

    @Override
    public boolean supportsCorrelatedSubqueries() throws SQLException {
        return databaseMetaData.supportsCorrelatedSubqueries();
    }

    @Override
    public boolean supportsUnion() throws SQLException {
        return databaseMetaData.supportsUnion();
    }

    @Override
    public boolean supportsUnionAll() throws SQLException {
        return databaseMetaData.supportsUnionAll();
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        return databaseMetaData.supportsOpenCursorsAcrossCommit();
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        return databaseMetaData.supportsOpenCursorsAcrossRollback();
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        return databaseMetaData.supportsOpenStatementsAcrossCommit();
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        return databaseMetaData.supportsOpenStatementsAcrossRollback();
    }

    @Override
    public int getMaxBinaryLiteralLength() throws SQLException {
        return databaseMetaData.getMaxBinaryLiteralLength();
    }

    @Override
    public int getMaxCharLiteralLength() throws SQLException {
        return databaseMetaData.getMaxCharLiteralLength();
    }

    @Override
    public int getMaxColumnNameLength() throws SQLException {
        return databaseMetaData.getMaxColumnNameLength();
    }

    @Override
    public int getMaxColumnsInGroupBy() throws SQLException {
        return databaseMetaData.getMaxColumnsInGroupBy();
    }

    @Override
    public int getMaxColumnsInIndex() throws SQLException {
        return databaseMetaData.getMaxColumnsInIndex();
    }

    @Override
    public int getMaxColumnsInOrderBy() throws SQLException {
        return databaseMetaData.getMaxColumnsInOrderBy();
    }

    @Override
    public int getMaxColumnsInSelect() throws SQLException {
        return databaseMetaData.getMaxColumnsInSelect();
    }

    @Override
    public int getMaxColumnsInTable() throws SQLException {
        return databaseMetaData.getMaxColumnsInTable();
    }

    @Override
    public int getMaxConnections() throws SQLException {
        return databaseMetaData.getMaxConnections();
    }

    @Override
    public int getMaxCursorNameLength() throws SQLException {
        return databaseMetaData.getMaxCursorNameLength();
    }

    @Override
    public int getMaxIndexLength() throws SQLException {
        return databaseMetaData.getMaxIndexLength();
    }

    @Override
    public int getMaxSchemaNameLength() throws SQLException {
        return databaseMetaData.getMaxSchemaNameLength();
    }

    @Override
    public int getMaxProcedureNameLength() throws SQLException {
        return databaseMetaData.getMaxProcedureNameLength();
    }

    @Override
    public int getMaxCatalogNameLength() throws SQLException {
        return databaseMetaData.getMaxCatalogNameLength();
    }

    @Override
    public int getMaxRowSize() throws SQLException {
        return databaseMetaData.getMaxRowSize();
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        return databaseMetaData.doesMaxRowSizeIncludeBlobs();
    }

    @Override
    public int getMaxStatementLength() throws SQLException {
        return databaseMetaData.getMaxStatementLength();
    }

    @Override
    public int getMaxStatements() throws SQLException {
        return databaseMetaData.getMaxStatements();
    }

    @Override
    public int getMaxTableNameLength() throws SQLException {
        return databaseMetaData.getMaxTableNameLength();
    }

    @Override
    public int getMaxTablesInSelect() throws SQLException {
        return databaseMetaData.getMaxTablesInSelect();
    }

    @Override
    public int getMaxUserNameLength() throws SQLException {
        return databaseMetaData.getMaxUserNameLength();
    }

    @Override
    public int getDefaultTransactionIsolation() throws SQLException {
        return databaseMetaData.getDefaultTransactionIsolation();
    }

    @Override
    public boolean supportsTransactions() throws SQLException {
        return databaseMetaData.supportsTransactions();
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        return databaseMetaData.supportsTransactionIsolationLevel(level);
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        return databaseMetaData.supportsDataDefinitionAndDataManipulationTransactions();
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        return databaseMetaData.supportsDataManipulationTransactionsOnly();
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        return databaseMetaData.dataDefinitionCausesTransactionCommit();
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        return databaseMetaData.dataDefinitionIgnoredInTransactions();
    }

    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {
        return databaseMetaData.getProcedures(catalog, schemaPattern, procedureNamePattern);
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException {
        return databaseMetaData.getProcedureColumns(catalog, schemaPattern, procedureNamePattern, columnNamePattern);
    }

    @Override
    @SneakyThrows
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        client.sendLog("catalog: " + catalog + ", " + "schemaPattern: " + schemaPattern + ", " + "tableNamePattern: " + tableNamePattern + ", " + "types: " + Arrays.toString(types));
        ResultSet tables = databaseMetaData.getTables(catalog, schemaPattern, tableNamePattern, types);
        CachedRowSet cachedTables = new CachedRowSetImpl();
        cachedTables.populate(tables);
        ResultSetData resultSetData = resultSetMapping.resultSetToDto(cachedTables);
        TablesToHide tablesToHide = client.getTablesToHide(resultSetData, db);
        resultSetModification.hide(cachedTables, tablesToHide.getHiddenTables());
        return cachedTables;
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        ResultSet schemas = databaseMetaData.getSchemas();
        CachedRowSet schemasResult = new CachedRowSetImpl();
        schemasResult.populate(schemas);
        client.sendQuery(client.resultSetToString(schemasResult));
        return schemasResult;
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        ResultSet catalogs = databaseMetaData.getCatalogs();
        CachedRowSet catalogsResult = new CachedRowSetImpl();
        catalogsResult.populate(catalogs);
        client.sendQuery(client.resultSetToString(catalogsResult));
        return catalogsResult;
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        ResultSet tableTypes = databaseMetaData.getTableTypes();
        CachedRowSet tableTypesCached = new CachedRowSetImpl();
        tableTypesCached.populate(tableTypes);
        client.sendQuery(client.resultSetToString(tableTypesCached));
        return tableTypesCached;
    }

    @Override
    @SneakyThrows
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        client.sendLog("catalog: " + catalog + ", " + "schemaPattern: " + schemaPattern + ", " + "tableNamePattern: " + tableNamePattern + ", " + "columnNamePattern: " + columnNamePattern);
        ResultSet columns = databaseMetaData.getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);
        CachedRowSet cachedColumns = new CachedRowSetImpl();
        cachedColumns.populate(columns);
        ResultSetData resultSetData = resultSetMapping.resultSetToDto(cachedColumns);
        ColumnsToHide columnsToHide = client.getColumnsToHide(resultSetData, db);
        resultSetModification.hide(cachedColumns, columnsToHide.getHiddenColumns());
        return cachedColumns;
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) throws SQLException {
        ResultSet columnsPrivileges = databaseMetaData.getColumnPrivileges(catalog, schema, table, columnNamePattern);
        client.sendQuery(client.resultSetToString(columnsPrivileges));
        return columnsPrivileges;
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        ResultSet tablePrivileges = databaseMetaData.getTablePrivileges(catalog, schemaPattern, tableNamePattern);
        client.sendQuery(client.resultSetToString(tablePrivileges));
        return tablePrivileges;
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) throws SQLException {
        ResultSet bestRowIdentifier = databaseMetaData.getBestRowIdentifier(catalog, schema, table, scope, nullable);
        client.sendQuery(client.resultSetToString(bestRowIdentifier));
        return bestRowIdentifier;
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        ResultSet versionColumns = databaseMetaData.getVersionColumns(catalog, schema, table);
        client.sendQuery(client.resultSetToString(versionColumns));
        return versionColumns;
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        ResultSet primaryKeys = databaseMetaData.getPrimaryKeys(catalog, schema, table);
        client.sendQuery(client.resultSetToString(primaryKeys));
        return primaryKeys;
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        ResultSet importedKeys = databaseMetaData.getImportedKeys(catalog, schema, table);
        client.sendQuery(client.resultSetToString(importedKeys));
        return importedKeys;
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        ResultSet exportedKeys = databaseMetaData.getExportedKeys(catalog, schema, table);
        client.sendQuery(client.resultSetToString(exportedKeys));
        return exportedKeys;
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
        ResultSet crossReference = databaseMetaData.getCrossReference(parentCatalog, parentSchema, parentTable, foreignCatalog, foreignSchema, foreignTable);
        client.sendQuery(client.resultSetToString(crossReference));
        return crossReference;
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        ResultSet typeInfo = databaseMetaData.getTypeInfo();
        client.sendQuery(client.resultSetToString(typeInfo));
        return typeInfo;
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException {
        ResultSet indexInfo = databaseMetaData.getIndexInfo(catalog, schema, table, unique, approximate);
        client.sendQuery(client.resultSetToString(indexInfo));
        return indexInfo;
    }

    @Override
    public boolean supportsResultSetType(int type) throws SQLException {
        return databaseMetaData.supportsResultSetType(type);
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
        return databaseMetaData.supportsResultSetConcurrency(type, concurrency);
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        return databaseMetaData.ownUpdatesAreVisible(type);
    }

    @Override
    public boolean ownDeletesAreVisible(int type) throws SQLException {
        return databaseMetaData.ownDeletesAreVisible(type);
    }

    @Override
    public boolean ownInsertsAreVisible(int type) throws SQLException {
        return databaseMetaData.ownInsertsAreVisible(type);
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        return databaseMetaData.othersUpdatesAreVisible(type);
    }

    @Override
    public boolean othersDeletesAreVisible(int type) throws SQLException {
        return databaseMetaData.othersDeletesAreVisible(type);
    }

    @Override
    public boolean othersInsertsAreVisible(int type) throws SQLException {
        return databaseMetaData.othersInsertsAreVisible(type);
    }

    @Override
    public boolean updatesAreDetected(int type) throws SQLException {
        return databaseMetaData.updatesAreDetected(type);
    }

    @Override
    public boolean deletesAreDetected(int type) throws SQLException {
        return databaseMetaData.deletesAreDetected(type);
    }

    @Override
    public boolean insertsAreDetected(int type) throws SQLException {
        return databaseMetaData.insertsAreDetected(type);
    }

    @Override
    public boolean supportsBatchUpdates() throws SQLException {
        return databaseMetaData.supportsBatchUpdates();
    }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) throws SQLException {
        ResultSet udts = databaseMetaData.getUDTs(catalog, schemaPattern, typeNamePattern, types);
        client.sendQuery(client.resultSetToString(udts));
        return udts;
    }

    @Override
    public Connection getConnection() throws SQLException {
        if(jdbcConnection.isClosed()) throw new SQLException("Connection closed");
        return jdbcConnection;
    }

    @Override
    public boolean supportsSavepoints() throws SQLException {
        return databaseMetaData.supportsSavepoints();
    }

    @Override
    public boolean supportsNamedParameters() throws SQLException {
        return databaseMetaData.supportsNamedParameters();
    }

    @Override
    public boolean supportsMultipleOpenResults() throws SQLException {
        return databaseMetaData.supportsMultipleOpenResults();
    }

    @Override
    public boolean supportsGetGeneratedKeys() throws SQLException {
        return databaseMetaData.supportsGetGeneratedKeys();
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        ResultSet superTypes = databaseMetaData.getSuperTypes(catalog, schemaPattern, typeNamePattern);
        client.sendQuery(client.resultSetToString(superTypes));
        return superTypes;
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        ResultSet superTables = databaseMetaData.getSuperTables(catalog, schemaPattern, tableNamePattern);
        client.sendQuery(client.resultSetToString(superTables));
        return superTables;
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) throws SQLException {
        ResultSet attributes = databaseMetaData.getAttributes(catalog, schemaPattern, typeNamePattern, attributeNamePattern);
        client.sendQuery(client.resultSetToString(attributes));
        return attributes;
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        return databaseMetaData.supportsResultSetHoldability(holdability);
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return databaseMetaData.getResultSetHoldability();
    }

    @Override
    public int getDatabaseMajorVersion() throws SQLException {
        return databaseMetaData.getDatabaseMajorVersion();
    }

    @Override
    public int getDatabaseMinorVersion() throws SQLException {
        return databaseMetaData.getDatabaseMinorVersion();
    }

    @Override
    public int getJDBCMajorVersion() throws SQLException {
        return databaseMetaData.getJDBCMajorVersion();
    }

    @Override
    public int getJDBCMinorVersion() throws SQLException {
        return databaseMetaData.getJDBCMinorVersion();
    }

    @Override
    public int getSQLStateType() throws SQLException {
        return databaseMetaData.getSQLStateType();
    }

    @Override
    public boolean locatorsUpdateCopy() throws SQLException {
        return databaseMetaData.locatorsUpdateCopy();
    }

    @Override
    public boolean supportsStatementPooling() throws SQLException {
        return databaseMetaData.supportsStatementPooling();
    }

    @Override
    public RowIdLifetime getRowIdLifetime() throws SQLException {
        return databaseMetaData.getRowIdLifetime();
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        ResultSet schemas = databaseMetaData.getSchemas(catalog, schemaPattern);
        CachedRowSet schemasResult = new CachedRowSetImpl();
        schemasResult.populate(schemas);
        client.sendQuery(client.resultSetToString(schemasResult));
        return schemasResult;
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        return databaseMetaData.supportsStoredFunctionsUsingCallSyntax();
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        return databaseMetaData.autoCommitFailureClosesAllResultSets();
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        return databaseMetaData.getClientInfoProperties();
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
        return databaseMetaData.getFunctions(catalog, schemaPattern, functionNamePattern);
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) throws SQLException {
        return databaseMetaData.getFunctionColumns(catalog, schemaPattern, functionNamePattern, columnNamePattern);
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        return databaseMetaData.getPseudoColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);
    }

    @Override
    public boolean generatedKeyAlwaysReturned() throws SQLException {
        return databaseMetaData.generatedKeyAlwaysReturned();
    }

    @Override
    public long getMaxLogicalLobSize() throws SQLException {
        return databaseMetaData.getMaxLogicalLobSize();
    }

    @Override
    public boolean supportsRefCursors() throws SQLException {
        return databaseMetaData.supportsRefCursors();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return databaseMetaData.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return databaseMetaData.isWrapperFor(iface);
    }
}
