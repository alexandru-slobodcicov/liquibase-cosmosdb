package liquibase.ext.cosmosdb.changelog;

import liquibase.ext.cosmosdb.statement.CountDocumentsInContainerStatement;

public class GetNextChangeSetSequenceValueStatement extends CountDocumentsInContainerStatement {

    public static final String COMMAND_NAME = "nextChangeSetSequence";

    public GetNextChangeSetSequenceValueStatement(final String containerName) {
        super(containerName);
    }

    @Override
    public String getCommandName() {
        return COMMAND_NAME;
    }

}
