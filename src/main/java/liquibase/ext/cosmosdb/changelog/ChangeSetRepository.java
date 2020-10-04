package liquibase.ext.cosmosdb.changelog;

import com.azure.cosmos.CosmosDatabase;
import liquibase.ext.cosmosdb.persistence.AbstractRepository;

public class ChangeSetRepository extends AbstractRepository<CosmosRanChangeSet> {

    public ChangeSetRepository(final CosmosDatabase database, final String containerName) {
        super(database.getContainer(containerName)
                , new ChangeSetToDocumentConverter());
    }
}
