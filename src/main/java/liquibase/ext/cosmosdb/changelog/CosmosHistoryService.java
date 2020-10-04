package liquibase.ext.cosmosdb.changelog;

/*-
 * #%L
 * Liquibase MongoDB Extension
 * %%
 * Copyright (C) 2019 Mastercard
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

import com.azure.cosmos.CosmosDatabase;
import liquibase.Scope;
import liquibase.changelog.AbstractChangeLogHistoryService;
import liquibase.changelog.ChangeSet;
import liquibase.changelog.RanChangeSet;
import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.exception.DatabaseHistoryException;
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.executor.ExecutorService;
import liquibase.ext.cosmosdb.database.CosmosConnection;
import liquibase.ext.cosmosdb.database.CosmosLiquibaseDatabase;
import liquibase.ext.cosmosdb.executor.CosmosExecutor;
import liquibase.ext.cosmosdb.statement.AbstractNoSqlStatement;
import liquibase.ext.cosmosdb.statement.CountContainersByNameStatement;
import liquibase.ext.cosmosdb.statement.DeleteContainerStatement;
import liquibase.logging.Logger;
import lombok.Getter;

import java.util.List;
import java.util.stream.Collectors;

import static java.util.Collections.unmodifiableList;
import static java.util.Objects.isNull;

public class CosmosHistoryService extends AbstractChangeLogHistoryService {

    private final Logger log = Scope.getCurrentScope().getLog(getClass());

    @Getter
    private List<RanChangeSet> ranChangeSetList;

    private boolean serviceInitialized;

    @Getter
    private Boolean hasDatabaseChangeLogTable;

    @Getter
    private Integer lastChangeSetSequenceValue;

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(Database database) {
        return CosmosLiquibaseDatabase.COSMOSDB_PRODUCT_NAME.equals(database.getDatabaseProductName());
    }

    @Override
    public void setDatabase(final Database database) {
        super.setDatabase(database);
    }

    @Override
    public CosmosLiquibaseDatabase getDatabase() {
        return (CosmosLiquibaseDatabase) super.getDatabase();
    }

    public String getDatabaseChangeLogTableName() {
        return getDatabase().getDatabaseChangeLogTableName();
    }

    public boolean canCreateChangeLogTable() {
        return true;
    }

    public boolean isServiceInitialized() {
        return serviceInitialized;
    }

    public CosmosExecutor getExecutor() {
        return (CosmosExecutor) Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor(CosmosExecutor.COSMOS_EXECUTOR_NAME, getDatabase());
    }

    @Override
    public void reset() {
        this.ranChangeSetList = null;
        this.serviceInitialized = false;
        this.hasDatabaseChangeLogTable = null;
    }

    @Override
    public void init() throws DatabaseException {

        final CosmosExecutor executor = getExecutor();

        if (this.serviceInitialized) {
            return;
        }

        final boolean createdTable = hasDatabaseChangeLogTable();

        if (createdTable) {
            //TODO: Add MD5SUM check logic and update not equal
        } else {
            log.info("Create Database Change Log Container");

            // If there is no table in the database for recording change history create one.
            this.log.info("Creating database history container with name: "
                    + ((CosmosConnection)getDatabase().getConnection()).getCosmosDatabase().getId() + "." + this.getDatabaseChangeLogTableName());

            final AbstractNoSqlStatement createChangeLogContainerStatement
                    = new CreateChangeLogContainerStatement(this.getDatabaseChangeLogTableName());

            executor.execute(createChangeLogContainerStatement);

            log.info("Created database history container : "
                    + createChangeLogContainerStatement.toJs());
            this.hasDatabaseChangeLogTable = true;
        }

        this.serviceInitialized = true;
    }

    public boolean hasDatabaseChangeLogTable() {
        if (isNull(this.hasDatabaseChangeLogTable)) {
            try {
                final CosmosExecutor executor = getExecutor();
                this.hasDatabaseChangeLogTable =
                        executor.queryForLong(new CountContainersByNameStatement(this.getDatabaseChangeLogTableName())) == 1L;
            } catch (final Exception e) {
                throw new UnexpectedLiquibaseException(e);
            }
        }
        return this.hasDatabaseChangeLogTable;
    }

    /**
     * Returns the ChangeSets that have been run against the current getDatabase().
     */
    @Override
    public List<RanChangeSet> getRanChangeSets() throws DatabaseException {

        if (isNull(this.ranChangeSetList)) {
            this.ranChangeSetList = getExecutor().queryForList(new SelectChangeLogRanChangeSetsStatement(getDatabaseChangeLogTableName()), RanChangeSet.class)
            .stream().map(RanChangeSet.class::cast).collect(Collectors.toList());
        }
        return unmodifiableList(ranChangeSetList);
    }

    @Override
    protected void replaceChecksum(final ChangeSet changeSet) throws DatabaseException {
//TODO: Implement
        //        final Document filter = new Document()
//                .append("fileName", changeSet.getFilePath())
//                .append("id", changeSet.getId())
//                .append("author", changeSet.getAuthor());
//
//        final Bson update = Updates.set(CHECKSUM_FIELD_NAME, changeSet.generateCheckSum().toString());

//        ((CosmosLiquibaseDatabase) getDatabase()).getConnection().getDb().getContainer(getDatabaseChangeLogTableName())
//                .updateOne(filter, update);

        log.info(String.format("Replace checksum executed. ChangeSet: [filename: %s, id: %s, author: %s]"
                , changeSet.getFilePath(), changeSet.getId(), changeSet.getAuthor()));

        reset();
    }

    @Override
    public RanChangeSet getRanChangeSet(final ChangeSet changeSet) throws DatabaseException, DatabaseHistoryException {
        if (!hasDatabaseChangeLogTable()) {
            return null;
        }
        return super.getRanChangeSet(changeSet);
    }

    @Override
    public void setExecType(final ChangeSet changeSet, final ChangeSet.ExecType execType) throws DatabaseException {

        final CosmosExecutor executor = getExecutor();

        final Integer nextSequenceValue = getNextSequenceValue();

        final MarkChangeSetRanStatement markChangeSetRanStatement =
                new MarkChangeSetRanStatement(getDatabaseChangeLogTableName(), changeSet, execType, nextSequenceValue, getDeploymentId());

        executor.execute(markChangeSetRanStatement);

        getDatabase().commit();
        if (this.ranChangeSetList != null) {
            this.ranChangeSetList.add(new RanChangeSet(changeSet, execType, null, null));
        }
    }

    @Override
    public void removeFromHistory(final ChangeSet changeSet) throws DatabaseException {
        //TODO: Implement

        //        final Document filter = new Document()
        //                .append("fileName", changeSet.getFilePath())
        //                .append("id", changeSet.getId())
        //                .append("author", changeSet.getAuthor());

        //TODO: implement with Executor of a statement
        //((CosmosLiquibaseDatabase) getDatabase()).getConnection().getDb().getContainer(getDatabaseChangeLogTableName())
        //        .deleteOne(filter);

        if (this.ranChangeSetList != null) {
            this.ranChangeSetList.remove(new RanChangeSet(changeSet));
        }
    }

    @Override
    public int getNextSequenceValue() throws DatabaseException {
        if (isNull(this.lastChangeSetSequenceValue)) {
            if (isNull(getDatabase().getConnection())) {
                this.lastChangeSetSequenceValue = 0;
            } else {
                this.lastChangeSetSequenceValue =
                        (int) getExecutor().queryForLong(new GetNextChangeSetSequenceValueStatement(getDatabaseChangeLogTableName()));
            }
        }

        this.lastChangeSetSequenceValue++;

        return this.lastChangeSetSequenceValue;
    }

    /**
     * Tags the database changelog with the given string.
     */
    @Override
    public void tag(final String tagString) throws DatabaseException {
        //        final long totalRows =
        //                ((CosmosLiquibaseDatabase) getDatabase()).getConnection().getDb().getContainer(getDatabaseChangeLogTableName())
        //                        .countDocuments();
        //        if (totalRows == 0L) {
        //            final ChangeSet emptyChangeSet = new ChangeSet(String.valueOf(new Date().getTime()), "liquibase",
        //                    false, false, "liquibase-internal", null, null,
        //                    getDatabase().getObjectQuotingStrategy(), null);
        //            this.setExecType(emptyChangeSet, ChangeSet.ExecType.EXECUTED);
        //        }

        //TODO: update the last row tag with TagDatabaseStatement tagString

        //        if (this.ranChangeSetList != null) {
        //            ranChangeSetList.get(ranChangeSetList.size() - 1).setTag(tagString);
        //        }
    }

    @Override
    public boolean tagExists(final String tag) {
        //final long count = ((CosmosLiquibaseDatabase) getDatabase()).getConnection().getDb().getContainer(getDatabaseChangeLogTableName())
        //        .countDocuments(new Document("tag", tag));
        //return count > 0L;
        return false;
    }

    @Override
    public void clearAllCheckSums() throws DatabaseException {
        //((CosmosLiquibaseDatabase) getDatabase()).getConnection().getDb().getContainer(getDatabaseChangeLogTableName())
        //       .updateMany(CLEAR_CHECKSUM_FILTER, CLEAR_CHECKSUM_UPDATE);

        log.info("Clear all checksums executed");
    }

    @Override
    public void destroy() {

        final CosmosExecutor executor = getExecutor();

        try {
            log.info("Dropping Container Database Change Log: " + getDatabaseChangeLogTableName());

            if (executor.queryForLong(new CountContainersByNameStatement(getDatabaseChangeLogTableName())) == 1L) {
                executor.execute(new DeleteContainerStatement(getDatabaseChangeLogTableName()));
                log.info("Dropped Container Database Change Log: " + getDatabaseChangeLogTableName());
            } else {
                log.warning("Cannot Drop Container Database Change Log as not found: " + getDatabaseChangeLogTableName());
            }
            reset();
        } catch (final DatabaseException e) {
            throw new UnexpectedLiquibaseException(e);
        }
    }
}
