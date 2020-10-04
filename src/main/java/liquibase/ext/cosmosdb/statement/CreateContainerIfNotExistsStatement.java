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
import com.azure.cosmos.models.CosmosContainerProperties;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@AllArgsConstructor
@Getter
@EqualsAndHashCode(callSuper = true)
public class CreateContainerIfNotExistsStatement extends CreateContainerStatement {

    public static final String COMMAND_NAME = "createContainerIfNotExists";

    public CreateContainerIfNotExistsStatement(final String containerName, final String options) {
        super(containerName, options);
    }

    @Override
    public String getCommandName() {
        return COMMAND_NAME;
    }

    @Override
    public void execute(final CosmosDatabase cosmosDatabase) {
        final CosmosContainerProperties cosmosContainerProperties = toContainerProperties(options);
        cosmosDatabase.createContainerIfNotExists(cosmosContainerProperties);
    }

    @Override
    public String toString() {
        return toJs();
    }
}
