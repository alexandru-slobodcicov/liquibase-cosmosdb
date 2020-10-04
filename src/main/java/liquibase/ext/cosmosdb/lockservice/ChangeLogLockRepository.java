package liquibase.ext.cosmosdb.lockservice;

import com.azure.cosmos.CosmosDatabase;
import liquibase.ext.cosmosdb.lockservice.ChangeLogLockToDocumentConverter;
import liquibase.ext.cosmosdb.lockservice.CosmosChangeLogLock;
import liquibase.ext.cosmosdb.persistence.AbstractRepository;

public class ChangeLogLockRepository extends AbstractRepository<CosmosChangeLogLock> {

    public ChangeLogLockRepository(final CosmosDatabase database, final String containerName) {
        super(database.getContainer(containerName)
                , new ChangeLogLockToDocumentConverter());
    }
}
