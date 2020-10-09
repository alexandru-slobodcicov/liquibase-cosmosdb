package liquibase.ext.cosmosdb.database;

/*-
 * #%L
 * Liquibase CosmosDB Extension
 * %%
 * Copyright (C) 2020 Mastercard
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import liquibase.CatalogAndSchema;
import liquibase.change.Change;
import liquibase.changelog.ChangeLogHistoryServiceFactory;
import liquibase.database.AbstractJdbcDatabase;
import liquibase.database.DatabaseConnection;
import liquibase.database.ObjectQuotingStrategy;
import liquibase.exception.DatabaseException;
import liquibase.exception.LiquibaseException;
import liquibase.exception.ValidationErrors;
import liquibase.executor.Executor;
import liquibase.executor.ExecutorService;
import liquibase.ext.cosmosdb.executor.CosmosExecutor;
import liquibase.ext.cosmosdb.statement.DeleteAllContainersStatement;
import liquibase.sql.visitor.SqlVisitor;
import liquibase.statement.DatabaseFunction;
import liquibase.statement.SqlStatement;
import liquibase.structure.DatabaseObject;

import java.io.IOException;
import java.io.Writer;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static java.util.Optional.ofNullable;

public class CosmosLiquibaseDatabase extends AbstractJdbcDatabase {

    public static final String COSMOSDB_PRODUCT_NAME = "Cosmos DB";
    public static final String COSMOSDB_PRODUCT_SHORT_NAME = "cosmosdb";
    public static final int DEFAULT_PORT = 8081;

    public CosmosLiquibaseDatabase() {
    }

    @Override
    public String getShortName() {
        return COSMOSDB_PRODUCT_SHORT_NAME;
    }

    @Override
    public Integer getDefaultScaleForNativeDataType(final String nativeDataType) {
        return null;
    }

    public Integer getDefaultPort() {
        return DEFAULT_PORT;
    }

    @Override
    public boolean supportsInitiallyDeferrableColumns() {
        return false;
    }

    @Override
    public boolean supportsSequences() {
        return false;
    }

    @Override
    public boolean supportsDropTableCascadeConstraints() {
        return false;
    }

    @Override
    public boolean supportsAutoIncrement() {
        return false;
    }

    @Override
    public String getLineComment() {
        return "<!-- -->";
    }

    @Override
    public boolean isSystemObject(final DatabaseObject example) {
        return false;
    }

    @Override
    public boolean isLiquibaseObject(final DatabaseObject object) {
        return false;
    }

    @Override
    public String getViewDefinition(final CatalogAndSchema schema, final String name) throws DatabaseException {
        return null;
    }

    @Override
    public String escapeObjectName(final String catalogName, final String schemaName, final String objectName, final Class<? extends DatabaseObject> objectType) {
        return null;
    }

    @Override
    public String escapeTableName(final String catalogName, final String schemaName, final String tableName) {
        return null;
    }

    @Override
    public String escapeIndexName(final String catalogName, final String schemaName, final String indexName) {
        return null;
    }

    @Override
    public String escapeObjectName(final String objectName, final Class<? extends DatabaseObject> objectType) {
        return null;
    }

    @Override
    public String escapeColumnName(final String catalogName, final String schemaName, final String tableName, final String columnName) {
        return null;
    }

    @Override
    public String escapeColumnName(final String catalogName, final String schemaName, final String tableName, final String columnName, final boolean quoteNamesThatMayBeFunctions) {
        return null;
    }

    @Override
    public String escapeColumnNameList(final String columnNames) {
        return null;
    }

    public boolean supportsTablespaces() {
        return false;
    }

    @Override
    public boolean supportsCatalogs() {
        return false;
    }

    @Override
    public CatalogAndSchema.CatalogAndSchemaCase getSchemaAndCatalogCase() {
        return CatalogAndSchema.CatalogAndSchemaCase.ORIGINAL_CASE;
    }

    @Override
    public boolean supportsSchemas() {
        return false;
    }

    @Override
    public boolean supportsCatalogInObjectName(final Class<? extends DatabaseObject> type) {
        return false;
    }

    @Override
    public String generatePrimaryKeyName(final String tableName) {
        return null;
    }

    @Override
    public String escapeSequenceName(final String catalogName, final String schemaName, final String sequenceName) {
        return null;
    }

    @Override
    public String escapeViewName(final String catalogName, final String schemaName, final String viewName) {
        return null;
    }

    @Override
    public void commit() throws DatabaseException {
        //TODO: implementation
    }

    @Override
    public void rollback() throws DatabaseException {
        //TODO: implementation
    }

    @Override
    public String escapeStringForDatabase(final String string) {
        return null;
    }

    @Override
    public void dropDatabaseObjects(final CatalogAndSchema schemaToDrop) throws LiquibaseException {
        final Executor executor = ExecutorService.getInstance().getExecutor(this);
        final DeleteAllContainersStatement deleteAllContainersStatement = new DeleteAllContainersStatement(Arrays.asList(getDatabaseChangeLogTableName(), getDatabaseChangeLogLockTableName()));
        executor.execute(deleteAllContainersStatement);
        ChangeLogHistoryServiceFactory.getInstance().getChangeLogService(this).destroy();
    }

    @Override
    public boolean supportsRestrictForeignKeys() {
        return false;
    }

    @Override
    public String escapeConstraintName(final String constraintName) {
        return null;
    }

    @Override
    public boolean isSafeToRunUpdate() throws DatabaseException {
        //TODO: Add the check to not be admin, etc.
        return false;
    }

    @Override
    public void saveStatements(final Change change, final List<SqlVisitor> sqlVisitors, final Writer writer) throws IOException {
        //TODO: implementation
    }

    @Override
    public void executeRollbackStatements(final Change change, final List<SqlVisitor> sqlVisitors) throws LiquibaseException {
        //TODO: implementation
    }

    @Override
    public void executeRollbackStatements(final SqlStatement[] statements, final List<SqlVisitor> sqlVisitors) throws LiquibaseException {
        //TODO: implementation
    }

    @Override
    public void saveRollbackStatement(final Change change, final List<SqlVisitor> sqlVisitors, final Writer writer) throws IOException, LiquibaseException {
        //TODO: implementation
    }

    @Override
    public List<DatabaseFunction> getDateFunctions() {
        //TODO: proper implementation
        return Collections.emptyList();
    }

    @Override
    public boolean supportsForeignKeyDisable() {
        return false;
    }

    @Override
    public boolean disableForeignKeyChecks() {
        return false;
    }

    @Override
    public void enableForeignKeyChecks() {
        //TODO: implementation
    }

    @Override
    public boolean isCaseSensitive() {
        return true;
    }

    @Override
    public boolean isReservedWord(final String string) {
        return false;
    }

    @Override
    public String correctObjectName(final String name, final Class<? extends DatabaseObject> objectType) {
        return null;
    }

    @Override
    public boolean isFunction(String string) {
        return false;
    }

    @Override
    public int getDataTypeMaxParameters(String dataTypeName) {
        return 0;
    }

    @Override
    public CatalogAndSchema getDefaultSchema() {
        return null;
    }

    @Override
    public boolean dataTypeIsNotModifiable(String typeName) {
        return false;
    }

    @Override
    public String generateDatabaseFunctionValue(DatabaseFunction databaseFunction) {
        return null;
    }

    @Override
    public ObjectQuotingStrategy getObjectQuotingStrategy() {
        return null;
    }

    @Override
    public void setObjectQuotingStrategy(final ObjectQuotingStrategy quotingStrategy) {
        //TODO: implementation
    }

    @Override
    public boolean createsIndexesForForeignKeys() {
        //Not applicable
        return false;
    }

    @Override
    public boolean getOutputDefaultSchema() {
        return false;
    }

    @Override
    public void setOutputDefaultSchema(final boolean outputDefaultSchema) {
        //TODO: implementation
    }

    @Override
    public boolean isDefaultSchema(final String catalog, final String schema) {
        return false;
    }

    @Override
    public boolean isDefaultCatalog(final String catalog) {
        return false;
    }

    @Override
    public boolean getOutputDefaultCatalog() {
        return false;
    }

    @Override
    public void setOutputDefaultCatalog(final boolean outputDefaultCatalog) {
        //TODO: implementation
    }

    @Override
    public boolean supportsPrimaryKeyNames() {
        //Not applicable
        return false;
    }

    @Override
    public boolean supportsNotNullConstraintNames() {
        //Not applicable
        return false;
    }

    @Override
    public boolean supportsBatchUpdates() {
        return false;
    }

    @Override
    public boolean requiresExplicitNullForColumns() {
        //Not applicable
        return false;
    }

    @Override
    public String getSystemSchema() {
        return null;
    }

    @Override
    public String escapeDataTypeName(final String dataTypeName) {
        return null;
    }

    @Override
    public String unescapeDataTypeName(final String dataTypeName) {
        return null;
    }

    @Override
    public String unescapeDataTypeString(final String dataTypeString) {
        return null;
    }

    @Override
    public ValidationErrors validate() {
        return null;
    }

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public String getDefaultDriver(final String url) {
        if (url.startsWith(CosmosConnectionString.COSMOSDB_PREFIX)) {
            return CosmosClientDriver.class.getName();
        }
        return null;
    }

    @Override
    public boolean requiresUsername() {
        return false;
    }

    @Override
    public boolean requiresPassword() {
        return false;
    }

    @Override
    public boolean getAutoCommitMode() {
        return false;
    }

    @Override
    public boolean supportsDDLInTransaction() {
        return false;
    }

    @Override
    public String getDatabaseProductName() {
        return COSMOSDB_PRODUCT_NAME;
    }

    protected String getDefaultDatabaseProductName() {
        return COSMOSDB_PRODUCT_NAME;
    }

    public boolean isCorrectDatabaseImplementation(final DatabaseConnection conn) throws DatabaseException {
        // If it looks like a CosmosDB, swims like a CosmosDB and quacks like a CosmosDB,
        return getDatabaseProductName().equals(conn.getDatabaseProductName());
    }

    @Override
    public String toString() {
        return getDatabaseProductName() + " : "
                + ofNullable(getConnection()).map(DatabaseConnection::getURL).orElse("NOT CONNECTED");
    }

}
