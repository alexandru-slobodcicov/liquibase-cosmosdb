package liquibase.ext.cosmosdb.statement;

import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.models.CosmosContainerProperties;

import java.util.Collections;
import java.util.List;

public class DeleteAllContainersStatement extends AbstractNoSqlStatement implements NoSqlExecuteStatement{

    public static final String COMMAND_NAME = "deleteAllContainers";

    final List<String> ignoreContainerNames;

    public DeleteAllContainersStatement() {
        this(Collections.emptyList());
    }

    public DeleteAllContainersStatement(final List<String> ignoreContainerNames) {
        this.ignoreContainerNames = ignoreContainerNames;
    }

    @Override
    public String getCommandName() {
        return COMMAND_NAME;
    }

    @Override
    public String toJs() {
        return
                "db." +
                        getCommandName() +
                        "(" +
                        ignoreContainerNames.toString() +
                        ");";
    }

    @Override
    public void execute(final CosmosDatabase cosmosDatabase) {
        cosmosDatabase.readAllContainers().stream()
                .map(CosmosContainerProperties::getId).filter(id -> !ignoreContainerNames.contains(id))
                .map(cosmosDatabase::getContainer).forEach(CosmosContainer::delete);
    }

    @Override
    public String toString() {
        return this.toJs();
    }
}
