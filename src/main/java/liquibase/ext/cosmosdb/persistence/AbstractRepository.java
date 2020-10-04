package liquibase.ext.cosmosdb.persistence;

import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.models.*;
import lombok.Getter;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static liquibase.ext.cosmosdb.persistence.AbstractItemToDocumentConverter.COSMOS_ID_FIELD;


public abstract class AbstractRepository<T> {

    public static final PartitionKey DEFAULT_PARTITION_KEY = new PartitionKey(AbstractItemToDocumentConverter.DEFAULT_PARTITION_KEY_VALUE);
    public static final String COSMOS_ID_PARAMETER = "@" + COSMOS_ID_FIELD;

    @Getter
    private final CosmosContainer container;

    @Getter
    private final AbstractItemToDocumentConverter<T, Map<String, Object>> converter;

    public AbstractRepository(final CosmosContainer container, final AbstractItemToDocumentConverter<T, Map<String, Object>> converter) {
        this.container = container;
        this.converter = converter;
    }

    public Optional<T> get(final String id) {

        final SqlQuerySpec querySpec = new SqlQuerySpec();
        querySpec.setQueryText(String.format("SELECT * FROM c WHERE c.id=\"%s\"", id));
        final CosmosQueryRequestOptions queryRequestOptions = new CosmosQueryRequestOptions();
        queryRequestOptions.setPartitionKey(DEFAULT_PARTITION_KEY);

        return container.queryItems(querySpec, queryRequestOptions, Map.class).stream().findFirst().map(converter::fromDocument);
    }

    public List<T> getAll() {
        return readAllItems().stream().map(converter::fromDocument).collect(Collectors.toList());
    }

    @SuppressWarnings("unchecked")
    public List<Map<String, Object>> readAllItems() {
        return container.readAllItems(DEFAULT_PARTITION_KEY, Map.class).stream()
                .map(i -> (Map<String, Object>)i).collect(Collectors.toList());
    }

    public int create(final Map<String, Object> document) {
        container.createItem(document, DEFAULT_PARTITION_KEY, null);
        return 1;
    }

    public int create(final T item) {
        final Map<String, Object> document = converter.toDocument(item);
        return create(document);
    }


    public int replace(final Map<String, Object> document) {
        container.replaceItem(document, (String)document.get(COSMOS_ID_FIELD), DEFAULT_PARTITION_KEY,  null);
        return 1;
    }

    public int replace(T item) {
        final Map<String, Object> document = converter.toDocument(item);
        return replace(document);
    }

    public int upsert(Map<String, Object> document) {
        container.upsertItem(document, DEFAULT_PARTITION_KEY, null);
        return 1;
    }

    public int upsert(final T item) {
        final Map<String, Object> document = converter.toDocument(item);
        return upsert(document);
    }

    public int delete(final Map<String, Object> document) {
        container.deleteItem((String)document.get(COSMOS_ID_FIELD), DEFAULT_PARTITION_KEY,  null);
        return 1;
    }

    public int delete(final T item) {
        final Map<String, Object> document = converter.toDocument(item);
        return delete(document);
    }

    public boolean exists(final String id) {
        final SqlQuerySpec querySpec
                = new SqlQuerySpec("SELECT * FROM c WHERE c.id=" + COSMOS_ID_PARAMETER, new SqlParameter(COSMOS_ID_PARAMETER, id));
        final CosmosQueryRequestOptions queryRequestOptions = new CosmosQueryRequestOptions();
        queryRequestOptions.setPartitionKey(DEFAULT_PARTITION_KEY);

        return 1L == container.queryItems(querySpec, queryRequestOptions, Map.class).stream().count();
    }

    public Integer count() {
        long count = container.readAllItems(DEFAULT_PARTITION_KEY, Map.class).stream().count();
        return (int) count;
    }

}
