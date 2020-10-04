package liquibase.ext.cosmosdb.statement;

import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.implementation.Document;
import com.azure.cosmos.models.SqlQuerySpec;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static liquibase.ext.cosmosdb.statement.JsonUtils.orEmptySqlQuerySpec;

public class UpdateEachItemStatement extends CreateItemStatement{

    public static final String COMMAND_NAME = "updateEachItem";

    private final SqlQuerySpec query;

    public UpdateEachItemStatement(final String containerName, final String jsonQuery, final String jsonDocument) {
        super(containerName, jsonDocument);
        this.query = orEmptySqlQuerySpec(jsonQuery);
    }

    public UpdateEachItemStatement(final String containerName, final SqlQuerySpec query, final Document document) {
        super(containerName, document);
        this.query = query;
    }

    public UpdateEachItemStatement() {
        this(null, (SqlQuerySpec)null, null);
    }

    @Override
    public void execute(final CosmosDatabase cosmosDatabase) {
        final CosmosContainer cosmosContainer = cosmosDatabase.getContainer(containerName);

        final Document source = getDocument();

        final List<Document> documents = cosmosContainer
                .queryItems(query, null, Map.class).stream().map(JsonUtils::fromMap)
                .map(Document.class::cast).map(d -> JsonUtils.mergeDocuments(d, source))
                .collect(Collectors.toList());

        documents.forEach(destination -> {
            JsonUtils.mergeDocuments(source, destination);
            cosmosContainer.upsertItem(destination);
        });
    }

    @Override
    public String getCommandName() {
        return COMMAND_NAME;
    }
}
