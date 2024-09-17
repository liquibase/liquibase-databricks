package liquibase.ext.databricks.datatype;

import liquibase.datatype.core.TimestampType;
import liquibase.Scope;
import liquibase.change.core.LoadDataChange;

import java.util.Locale;

import liquibase.GlobalConfiguration;
import liquibase.database.Database;
import liquibase.database.core.*;
import liquibase.datatype.DataTypeInfo;
import liquibase.datatype.DatabaseDataType;
import liquibase.exception.DatabaseIncapableOfOperation;
import liquibase.servicelocator.PrioritizedService;
import liquibase.util.grammar.ParseException;
import liquibase.ext.databricks.database.DatabricksDatabase;
import org.apache.commons.lang3.StringUtils;

/**
 * Data type support for TIMESTAMP data types in various DBMS. All DBMS are at least expected to support the
 * year, month, day, hour, minute and second parts. Optionally, fractional seconds and time zone information can be
 * specified as well.
 */

//TODO refactor to simplify this class
@DataTypeInfo(name = "timestamp", aliases = {"java.sql.Types.TIMESTAMP", "java.sql.Types.TIMESTAMP_WITH_TIMEZONE", "java.sql.Timestamp", "timestamptz"},
        minParameters = 0, maxParameters = 0, priority = PrioritizedService.PRIORITY_DATABASE)
public class TimestampDatatypeDatabricks extends TimestampType {

    /**
     * Returns a DBMS-specific String representation of this TimestampType for use in SQL statements.
     *
     * @param database the database for which the String must be generated
     * @return a String with the DBMS-specific type specification
     */
    @Override
    public DatabaseDataType toDatabaseDataType(Database database) {
        String originalDefinition = StringUtils.trimToEmpty(getRawDefinition());
        // If a fractional precision is given, check is the DBMS supports the length
        if (getParameters().length > 0) {
            Integer desiredLength = null;
            try {
                desiredLength = Integer.parseInt(String.valueOf(getParameters()[0]));
            } catch (NumberFormatException e) {
                // That is ok, we won't touch the parameter in this case.
            }

            if (desiredLength != null) {
                int maxFractionalDigits = database.getMaxFractionalDigitsForTimestamp();
                if (maxFractionalDigits < desiredLength) {
                    throw new DatabaseIncapableOfOperation(
                            String.format(
                                    "Using a TIMESTAMP data type with a fractional precision of %d", desiredLength
                            ),
                            String.format(
                                    "A timestamp datatype with %d fractional digits was requested, but %s " +
                                            "only supports %d digits.",
                                    desiredLength,
                                    database.getDatabaseProductName(),
                                    maxFractionalDigits
                            ),
                            database
                    );
                }
            }
        }

        /*
         * From here on, we assume that we have a SQL standard compliant database that supports the
         * TIMESTAMP[(p)] [WITH TIME ZONE|WITHOUT TIME ZONE] syntax. p is the number of fractional digits,
         * i.e. if "2017-06-02 23:59:45.123456" is supported by the DBMS, p would be 6.
         */
        DatabaseDataType type;

        if (getParameters().length > 0 && !(database instanceof SybaseASADatabase)) {
            int fractionalDigits = 0;
            String fractionalDigitsInput = getParameters()[0].toString();
            try {
                fractionalDigits = Integer.parseInt(fractionalDigitsInput);
            } catch (NumberFormatException e) {
                throw new RuntimeException(
                        new ParseException(String.format("A timestamp with '%s' fractional digits was requested, but '%s' does not " +
                                "seem to be an integer.", fractionalDigitsInput, fractionalDigitsInput))
                );
            }
            int maxFractionalDigits = database.getMaxFractionalDigitsForTimestamp();
            if (maxFractionalDigits < fractionalDigits) {
                Scope.getCurrentScope().getLog(getClass()).warning(String.format(
                        "A timestamp datatype with %d fractional digits was requested, but the DBMS %s only supports " +
                                "%d digits. Because of this, the number of digits was reduced to %d.",
                        fractionalDigits, database.getDatabaseProductName(), maxFractionalDigits, maxFractionalDigits)
                );
                fractionalDigits = maxFractionalDigits;
            }
            // Do not return parameter p for Databricks
            type = new DatabaseDataType("TIMESTAMP");
        } else {
            type = new DatabaseDataType("TIMESTAMP");
        }

        if (originalDefinition.startsWith("java.sql.Types.TIMESTAMP_WITH_TIMEZONE")
                && (database instanceof PostgresDatabase
                || database instanceof OracleDatabase
                || database instanceof H2Database
                || database instanceof HsqlDatabase
                || database instanceof SybaseASADatabase)) {

            if (database instanceof PostgresDatabase
                    || database instanceof H2Database
                    || database instanceof SybaseASADatabase) {
                type.addAdditionalInformation("WITH TIME ZONE");
            } else {
                type.addAdditionalInformation("WITH TIMEZONE");
            }

            return type;
        }

        if (getAdditionalInformation() != null
                && (database instanceof PostgresDatabase
                || database instanceof OracleDatabase)
                || database instanceof H2Database
                || database instanceof HsqlDatabase
                || database instanceof SybaseASADatabase) {
            String additionalInformation = this.getAdditionalInformation();

            if (additionalInformation != null) {
                String additionInformation = additionalInformation.toUpperCase(Locale.US);
                if ((database instanceof PostgresDatabase || database instanceof H2Database || database instanceof SybaseASADatabase)
                        && additionInformation.toUpperCase(Locale.US).contains("TIMEZONE")) {
                    additionalInformation = additionInformation.toUpperCase(Locale.US).replace("TIMEZONE", "TIME ZONE");
                }

            }

            type.addAdditionalInformation(additionalInformation);
            return type;
        }

        return super.toDatabaseDataType(database);
    }

    @Override
    public boolean supports(Database database) {
        return database instanceof DatabricksDatabase;
    }
}