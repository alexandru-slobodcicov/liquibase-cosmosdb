package liquibase.ext.cosmosdb.changelog;

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

import com.azure.cosmos.implementation.Document;
import com.azure.cosmos.models.SqlQuerySpec;
import liquibase.ChecksumVersion;
import liquibase.Scope;
import liquibase.change.Change;
import liquibase.change.core.TagDatabaseChange;
import liquibase.changelog.ChangeSet;
import liquibase.changelog.RanChangeSet;
import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.ext.cosmosdb.database.CosmosLiquibaseDatabase;
import liquibase.ext.cosmosdb.statement.*;
import liquibase.logging.Logger;
import liquibase.nosql.changelog.AbstractNoSqlHistoryService;
import liquibase.nosql.executor.NoSqlExecutor;
import liquibase.util.StringUtil;

import java.util.List;
import java.util.stream.Collectors;

public class CosmosHistoryService extends AbstractNoSqlHistoryService<CosmosLiquibaseDatabase> {

    private final Logger log = Scope.getCurrentScope().getLog(getClass());

    @Override
    public int getPriority() {
        return PRIORITY_SPECIALIZED;
    }

    @Override
    protected Logger getLogger() {
        return log;
    }

    @Override
    public boolean supports(final Database database) {
        getLogger().fine("Entering: " + getClass().getSimpleName() + " supports()");
        return CosmosLiquibaseDatabase.COSMOSDB_PRODUCT_NAME.equals(database.getDatabaseProductName());
    }

    @Override
    public List<RanChangeSet> getRanChangeSets(boolean b) throws DatabaseException {
        return getRanChangeSets();
    }

    @Override
    public boolean isDatabaseChecksumsCompatible() {
        return false;
    }

    @Override
    protected Boolean existsRepository() throws DatabaseException {
        getLogger().fine("Entering: " + getClass().getSimpleName() + " existsRepository()");
        return getExecutor().queryForLong(
                new CountContainersByNameStatement(this.getDatabaseChangeLogTableName())) == 1L;
    }

    @Override
    protected void createRepository() throws DatabaseException {
        getLogger().fine("Entering: " + getClass().getSimpleName() + " createRepository()");
        final CreateChangeLogContainerStatement createChangeLogContainerStatement =
                new CreateChangeLogContainerStatement(this.getDatabaseChangeLogTableName());
        getExecutor().execute(createChangeLogContainerStatement);

    }

    @Override
    protected void adjustRepository() throws DatabaseException {
        //NOOP
    }

    @Override
    protected void dropRepository() throws DatabaseException {
        getLogger().fine("Entering: " + getClass().getSimpleName() + " dropRepository()");
        getExecutor().execute(
                new DeleteContainerStatement(getDatabaseChangeLogTableName()));
    }

    @Override
    protected List<RanChangeSet> queryRanChangeSets() throws DatabaseException {
        getLogger().fine("Entering: " + getClass().getSimpleName() + " queryRanChangeSets()");
        return getExecutor().queryForList(new SelectChangeLogRanChangeSetsStatement(getDatabaseChangeLogTableName()), RanChangeSet.class)
                .stream().map(RanChangeSet.class::cast).collect(Collectors.toList());
    }

    @Override
    protected Integer generateNextSequence() throws DatabaseException {
        getLogger().fine("Entering: " + getClass().getSimpleName() + " generateNextSequence()");
        return (int) getExecutor().queryForLong(new GetNextChangeSetSequenceValueStatement(getDatabaseChangeLogTableName()));
    }

    @Override
    protected void markChangeSetRun(final ChangeSet changeSet, final ChangeSet.ExecType execType, final Integer nextSequenceValue)
            throws DatabaseException {
        getLogger().fine("Entering: " + getClass().getSimpleName() + " markChangeSetRun(cs,et,nxtseq)");
        final NoSqlExecutor executor = getExecutor();

        final MarkChangeSetRanStatement markChangeSetRanStatement =
                new MarkChangeSetRanStatement(getDatabaseChangeLogTableName(), changeSet, execType, nextSequenceValue, getDeploymentId());

        executor.execute(markChangeSetRanStatement);

    }

    //TODO: Raise with Liquibase to make it as part of ChangeSet class
    public String extractTag(final ChangeSet changeSet) {
        getLogger().fine("Entering: " + getClass().getSimpleName() + " extractTag(changeSet)");
        String tag = null;
        for (Change change : changeSet.getChanges()) {
            if (change instanceof TagDatabaseChange) {
                TagDatabaseChange tagChange = (TagDatabaseChange) change;
                tag = StringUtil.trimToNull(tagChange.getTag());
            }
        }
        return tag;
    }

    @Override
    protected void removeRanChangeSet(final ChangeSet changeSet) throws DatabaseException {
        getLogger().fine("Entering: " + getClass().getSimpleName() + " removeRanChangeSet(changeSet)");
        String query = "SELECT * FROM c where c.author=\"" + changeSet.getAuthor() + "\" and c.changeSetId=\""+ changeSet.getId()+ "\" and c.fileName=\""+ changeSet.getFilePath() + "\"";
        SqlQuerySpec querySpec = new SqlQuerySpec(query);
        getExecutor().execute(
            new DeleteEachItemStatement(getDatabaseChangeLogTableName(), 
            querySpec));
    }

    @Override
    protected void clearCheckSums() throws DatabaseException {
        getLogger().fine("Entering: " + getClass().getSimpleName() + " clearCheckSums()");
        String query = "SELECT * FROM c";
        //Nullify each md5sum field
        String docString = "{\n" +
            "\"" + CosmosRanChangeSet.Fields.LAST_CHECK_SUM + "\"" + " : null\n" +
        "}";
        Document doc = new Document(docString);
        SqlQuerySpec querySpec = new SqlQuerySpec(query);
        getExecutor().execute(
            new UpdateEachItemStatement(getDatabaseChangeLogTableName(), 
            querySpec,
            doc));
    }

    @Override
    protected long countTags(final String tag) throws DatabaseException {
        getLogger().fine("Entering: " + getClass().getSimpleName() + " countTags(tag)");
        //TODO: Implement
        return 0;
    }

    @Override
    protected void tagLast(final String tagString) throws DatabaseException {
        getLogger().fine("Entering: " + getClass().getSimpleName() + " tagLast(tag)");
        String query = "SELECT TOP 1 * FROM c order by c.dateExecuted DESC";
        SqlQuerySpec querySpec = new SqlQuerySpec(query);
        String docString = "{\n" +
            "\"" + CosmosRanChangeSet.Fields.TAG + "\"" + " : \"" + tagString + "\"\n" +
        "}";
        Document doc = new Document(docString);
        getExecutor().execute(
            new UpdateEachItemStatement(getDatabaseChangeLogTableName(), 
            querySpec,
            doc));
    }

    @Override
    protected long countRanChangeSets() throws DatabaseException {
        getLogger().fine("Entering: " + getClass().getSimpleName() + " countRanChangeSets()");
        return getExecutor().queryForLong(new CountDocumentsInContainerStatement(getDatabaseChangeLogTableName()));
    }

    @Override
    protected void updateCheckSum(final ChangeSet changeSet) throws DatabaseException {
        getLogger().fine("Entering: " + getClass().getSimpleName() + " updateCheckSum(changeSet)");
        String query = "SELECT * FROM c where c.author=\"" + changeSet.getAuthor() + "\" and c.changeSetId=\""+ changeSet.getId()+ "\" and c.fileName=\""+ changeSet.getFilePath() + "\"";
        SqlQuerySpec querySpec = new SqlQuerySpec(query);
        String docString = "{\n" +
            "\"" + CosmosRanChangeSet.Fields.LAST_CHECK_SUM + "\"" + " : \"" + changeSet.generateCheckSum(ChecksumVersion.latest()) + "\"\n" +
        "}";
        Document doc = new Document(docString);
        getExecutor().execute(
            new UpdateEachItemStatement(getDatabaseChangeLogTableName(), 
            querySpec,
            doc));
    }
}
