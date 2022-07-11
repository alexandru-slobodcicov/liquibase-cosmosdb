package liquibase.ext.cosmosdb.persistence;

import liquibase.util.StringUtil;

import java.text.SimpleDateFormat;
import java.util.Date;

import static java.util.Objects.isNull;

public abstract class AbstractItemToDocumentConverter<A, B> {

    public static final String ISO_8601_UTC_DATETIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS";
    public static final SimpleDateFormat dateFormatter = new SimpleDateFormat(ISO_8601_UTC_DATETIME_FORMAT);

    public static final String AND = " AND ";
    public static final String COMMA = ",";
    public static final String WHITESPACE = " ";
    public static final String OPEN_BRACKET = "(";
    public static final String CLOSE_BRACKET = ")";

    public abstract B toDocument(A item);

    public abstract A fromDocument(B document);

    public Date toDate(final String dateString) {
        try {
            if (isNull(StringUtil.trimToNull(dateString))) {
                return null;
            }
            return dateFormatter.parse(dateString);
        } catch (final Exception e) {
            throw new RuntimeException("Cannot parse to Date: [" + dateString + "] with pattern: " +
                    AbstractItemToDocumentConverter.ISO_8601_UTC_DATETIME_FORMAT, e);
        }
    }

    public String fromDate(final Date date) {
        if (isNull(date)) {
            return null;
        }
        return dateFormatter.format(date);
    }

}
