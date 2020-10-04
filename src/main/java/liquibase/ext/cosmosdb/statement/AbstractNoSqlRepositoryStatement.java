package liquibase.ext.cosmosdb.statement;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
public abstract class AbstractNoSqlRepositoryStatement extends AbstractNoSqlStatement {

    public static final Integer ITEM_ID_1 = 1;
    public static final String ITEM_ID_1_STRING = "1";

    @Getter
    protected final String containerName;

    public AbstractNoSqlRepositoryStatement() {
        this(null);
    }

    @Override
    public String toJs() {
        return "db." +
                getCommandName() +
                "(" +
                containerName +
                ");";
    }
}
