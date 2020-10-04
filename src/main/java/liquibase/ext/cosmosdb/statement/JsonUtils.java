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

import com.azure.cosmos.implementation.JsonSerializable;
import com.azure.cosmos.implementation.Utils;
import com.azure.cosmos.models.SqlParameter;
import com.azure.cosmos.models.SqlQuerySpec;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.NoArgsConstructor;
import com.azure.cosmos.implementation.Document;

import java.util.Map;

import static java.util.Optional.ofNullable;
import static liquibase.util.StringUtil.trimToNull;
import static lombok.AccessLevel.PRIVATE;

@NoArgsConstructor(access = PRIVATE)
public final class JsonUtils {
    public static final ObjectMapper OBJECT_MAPPER = Utils.getSimpleObjectMapper();



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

    public static Document fromMap(final Map<?, ?> source) {
        return Document.fromObject(source, OBJECT_MAPPER);
    }

    public static Document mergeDocuments(final Document destination, final Document source) {
        destination.getPropertyBag().setAll(source.getPropertyBag().deepCopy());
        return destination;
    }

}
