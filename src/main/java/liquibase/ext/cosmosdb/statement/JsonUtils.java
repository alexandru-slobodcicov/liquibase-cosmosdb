package liquibase.ext.cosmosdb.statement;

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

import com.azure.cosmos.implementation.DocumentCollection;
import com.azure.cosmos.implementation.JsonSerializable;
import com.azure.cosmos.implementation.Utils;
import com.azure.cosmos.models.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.NoArgsConstructor;
import com.azure.cosmos.implementation.Document;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;

import static java.util.Objects.nonNull;
import static java.util.Optional.ofNullable;
import static liquibase.util.StringUtil.trimToNull;
import static lombok.AccessLevel.PRIVATE;

@NoArgsConstructor(access = PRIVATE)
public final class JsonUtils {
    public static final ObjectMapper OBJECT_MAPPER = Utils.getSimpleObjectMapper();
    public static final String DEFAULT_PARTITION_KEY_NAME = "null";
    public static final String DEFAULT_PARTITION_KEY_PATH = "/" + DEFAULT_PARTITION_KEY_NAME;
    public static final PartitionKey DEFAULT_PARTITION_KEY = new PartitionKey("default");
    public static final PartitionKey DEFAULT_PARTITION_KEY_PERSIST = PartitionKey.NONE;
    public static final String COSMOS_ID_FIELD = "id";
    public static final String COSMOS_ID_PARAMETER = "@" + COSMOS_ID_FIELD;
    public static final String QUERY_SELECT_ALL = "SELECT * FROM c";

    public static Document orEmptyDocument(final String json) {
        return ofNullable(trimToNull(json)).map(Document::new)
                .orElseGet(Document::new);
    }

    /**
     * Deserialize the json to query parameters.
     *
     * @param json the query parameters in json format.
     *             See request body: https://docs.microsoft.com/en-us/rest/api/cosmos-db/query-documents.
     * @return the {@link SqlQuerySpec}.
     */
    public static SqlQuerySpec orEmptySqlQuerySpec(final String json) {

        return ofNullable(trimToNull(json)).map(JsonSerializable::new)
                .map(js -> {
                    final SqlQuerySpec querySpec = new SqlQuerySpec();
                    querySpec.setQueryText(js.getString("query"));
                    querySpec.setParameters(js.getList("parameters", SqlParameter.class));
                    return querySpec;
                }).orElseGet(SqlQuerySpec::new);
    }

    /**
     * Deserialize the json to Stored Procedure parameters.
     *
     * @param json the Stored Procedure in json format.
     *             See request body: https://docs.microsoft.com/en-us/rest/api/cosmos-db/create-a-stored-procedure.
     * @return the {@link CosmosStoredProcedureProperties}.
     */
    public static CosmosStoredProcedureProperties orEmptyStoredProcedureProperties(final String json) {

        return ofNullable(trimToNull(json)).map(JsonSerializable::new)
                .map(js -> new CosmosStoredProcedureProperties(
                 js.getString("id"),
                js.getString("body"))).orElse(new CosmosStoredProcedureProperties(null, null));
    }

    public static Document fromMap(final Map<?, ?> source) {
        return Document.fromObject(source, OBJECT_MAPPER);
    }

    public static Document mergeDocuments(final Document destination, final Document source) {
        destination.getPropertyBag().setAll(source.getPropertyBag().deepCopy());
        return destination;
    }

    public static CosmosContainerProperties toContainerProperties(final String containerName, final String optionsJson) {

        final CosmosContainerProperties cosmosContainerProperties = new CosmosContainerProperties(containerName, DEFAULT_PARTITION_KEY_PATH);
        if (StringUtils.isNotEmpty(optionsJson)) {
            final DocumentCollection documentCollection = new DocumentCollection(optionsJson);
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
}
