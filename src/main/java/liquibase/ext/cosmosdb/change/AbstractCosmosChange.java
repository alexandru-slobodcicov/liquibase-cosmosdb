package liquibase.ext.cosmosdb.change;

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

import liquibase.change.AbstractChange;
import liquibase.database.Database;
import liquibase.ext.cosmosdb.database.CosmosLiquibaseDatabase;
import lombok.Setter;

public abstract class AbstractCosmosChange extends AbstractChange {

    @Override
    public boolean supports(final Database database) {
        return database instanceof CosmosLiquibaseDatabase;
    }

}
