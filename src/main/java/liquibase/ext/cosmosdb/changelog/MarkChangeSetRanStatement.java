package liquibase.ext.cosmosdb.changelog;

import com.azure.cosmos.CosmosDatabase;
import liquibase.change.Change;
import liquibase.change.core.TagDatabaseChange;
import liquibase.changelog.ChangeSet;
import liquibase.changelog.RanChangeSet;
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.ext.cosmosdb.statement.AbstractNoSqlContainerStatement;
import liquibase.ext.cosmosdb.statement.NoSqlExecuteStatement;
import liquibase.util.LiquibaseUtil;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

import java.util.Date;
import java.util.UUID;

public class MarkChangeSetRanStatement extends AbstractNoSqlContainerStatement implements NoSqlExecuteStatement {

    public static final String COMMAND_NAME = "markChangeSet";

    @Getter
    private final ChangeSet changeSet;

    @Getter
    private final ChangeSet.ExecType execType;

    @Getter
    private final Integer orderExecuted;

    @Getter
    private final String deploymentId;

    public MarkChangeSetRanStatement(final String containerName, final ChangeSet changeSet, final ChangeSet.ExecType execType
            , final Integer orderExecuted, final String deploymentId) {
        super(containerName);
        this.changeSet = changeSet;
        this.execType = execType;
        this.orderExecuted = orderExecuted;
        this.deploymentId = deploymentId;
    }

    @Override
    public String getCommandName() {
        return COMMAND_NAME;
    }

    @Override
    public String toJs() {
        return
                "db."
                        + getCommandName()
                        + "("
                        + containerName
                        + ", "
                        + changeSet
                        + ", "
                        + execType
                        + ", "
                        + orderExecuted
                        + ", "
                        + deploymentId
                        + ");";
    }

    @Override
    public void execute(final CosmosDatabase cosmosDatabase) {
        try {
            final ChangeSetRepository repository = new ChangeSetRepository(cosmosDatabase, containerName);

            if (execType.equals(ChangeSet.ExecType.FAILED) || execType.equals(ChangeSet.ExecType.SKIPPED)) {
                return; //don't mark
            }


            if (execType.ranBefore) {
                //TODO: update
                final RanChangeSet updateRanChangeSet = null;
//            runStatement = new UpdateStatement(database.getLiquibaseCatalogName(), database.getLiquibaseSchemaName(), database.getDatabaseChangeLogTableName())
//                    .addNewColumnValue("DATEEXECUTED", new DatabaseFunction(dateValue))
//                    .addNewColumnValue("ORDEREXECUTED", ChangeLogHistoryServiceFactory.getInstance().getChangeLogService(database).getNextSequenceValue())
//                    .addNewColumnValue("MD5SUM", changeSet.generateCheckSum().toString())
//                    .addNewColumnValue("EXECTYPE", execType.value)
//                    .addNewColumnValue("DEPLOYMENT_ID", ChangeLogHistoryServiceFactory.getInstance().getChangeLogService(database).getDeploymentId())
//                    .setWhereClause(database.escapeObjectName("ID", LiquibaseColumn.class) + " = ? " +
//                            "AND " + database.escapeObjectName("AUTHOR", LiquibaseColumn.class) + " = ? " +
//                            "AND " + database.escapeObjectName("FILENAME", LiquibaseColumn.class) + " = ?")
//                    .addWhereParameters(changeSet.getId(), changeSet.getAuthor(), changeSet.getFilePath());
//
//            if (tag != null) {
//                ((UpdateStatement) runStatement).addNewColumnValue("TAG", tag);
//            }
            } else {

                final CosmosRanChangeSet insertRanChangeSet = new CosmosRanChangeSet(
                        UUID.randomUUID().toString()
                        , changeSet.getFilePath()
                        , changeSet.getId()
                        , changeSet.getAuthor()
                        , changeSet.generateCheckSum()
                        , new Date()
                        , extractTag(changeSet)
                        , execType
                        , changeSet.getDescription()
                        , changeSet.getComments()
                        , changeSet.getContexts()
                        , changeSet.getInheritableContexts()
                        , changeSet.getLabels()
                        , deploymentId
                        , orderExecuted
                        , LiquibaseUtil.getBuildVersion()
                );

                repository.create(insertRanChangeSet);
            }

            } catch(final Exception e){
                throw new UnexpectedLiquibaseException(e);
            }

        }

        public String extractTag(final ChangeSet changeSet) {
            String tag = null;
            for (Change change : changeSet.getChanges()) {
                if (change instanceof TagDatabaseChange) {
                    TagDatabaseChange tagChange = (TagDatabaseChange) change;
                    tag = StringUtils.trimToNull(tagChange.getTag());
                }
            }
            return tag;
        }
    }


