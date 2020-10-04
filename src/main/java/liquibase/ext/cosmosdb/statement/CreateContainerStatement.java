package liquibase.ext.cosmosdb.statement;

/*-
 * #%L
 * Liquibase MongoDB Extension
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

import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.implementation.DocumentCollection;
import com.azure.cosmos.models.CosmosContainerProperties;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;

import static java.util.Objects.nonNull;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@EqualsAndHashCode(callSuper = true)
public class CreateContainerStatement extends AbstractNoSqlStatement implements NoSqlExecuteStatement {

    public static final String DEFAULT_PARTITION_KEY_NAME = "partition";
    public static final String DEFAULT_PARTITION_KEY_PATH = "/" + DEFAULT_PARTITION_KEY_NAME;

    public static final String COMMAND_NAME = "createContainer";

    protected String containerName;
    protected String options;

    public CreateContainerStatement(final String containerName) {
        this(containerName, null);
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
                        + options
                        + ");";
    }

    @Override
    public void execute(final CosmosDatabase cosmosDatabase) {

        final CosmosContainerProperties cosmosContainerProperties = toContainerProperties(options);
        cosmosDatabase.createContainer(cosmosContainerProperties);
    }

    protected CosmosContainerProperties toContainerProperties(final String options) {

        final CosmosContainerProperties cosmosContainerProperties = new CosmosContainerProperties(containerName, DEFAULT_PARTITION_KEY_PATH);
        if (StringUtils.isNotEmpty(options)) {
            final DocumentCollection documentCollection = new DocumentCollection(options);
            if(nonNull(documentCollection.getPartitionKey())) {
                cosmosContainerProperties.setPartitionKeyDefinition(documentCollection.getPartitionKey());
            }
            if(nonNull(documentCollection.getIndexingPolicy())) {
                cosmosContainerProperties.setIndexingPolicy(documentCollection.getIndexingPolicy());
            }
            if(nonNull(documentCollection.getUniqueKeyPolicy())) {
                cosmosContainerProperties.setUniqueKeyPolicy(documentCollection.getUniqueKeyPolicy());
            }
            if(nonNull(documentCollection.getAnalyticalStoreTimeToLiveInSeconds())) {
                cosmosContainerProperties.setAnalyticalStoreTimeToLiveInSeconds(documentCollection.getAnalyticalStoreTimeToLiveInSeconds());
            }
            if(nonNull(documentCollection.getDefaultTimeToLive())) {
                cosmosContainerProperties.setDefaultTimeToLiveInSeconds(documentCollection.getDefaultTimeToLive());
            }
            if(nonNull(documentCollection.getConflictResolutionPolicy())) {
                cosmosContainerProperties.setConflictResolutionPolicy(documentCollection.getConflictResolutionPolicy());
            }
        }
        return cosmosContainerProperties;
    }

    @Override
    public String toString() {
        return toJs();
    }
}
