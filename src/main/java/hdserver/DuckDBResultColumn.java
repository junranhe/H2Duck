package hdserver;

import org.h2.engine.Constants;
import org.h2.result.ResultInterface;
import org.h2.value.TypeInfo;
import java.io.IOException;

/**
 * A result set column of a remote result.
 */
public class DuckDBResultColumn {

    /**
     * The column alias.
     */
    final String alias;

    /**
     * The schema name or null.
     */
    final String schemaName;

    /**
     * The table name or null.
     */
    final String tableName;

    /**
     * The column name or null.
     */
    final String columnName;

    /**
     * The column type.
     */
    final TypeInfo columnType;

    /**
     * True if this is an identity column.
     */
    final boolean identity;

    /**
     * True if this column is nullable.
     */
    final int nullable;

    /**
     * Read an object from the given transfer object.
     *
     * @param in the object from where to read the data
     */
    DuckDBResultColumn(DuckDBTransfer in) throws IOException {
        alias = in.readString();
        schemaName = in.readString();
        tableName = in.readString();
        columnName = in.readString();
        columnType = in.readTypeInfo();
        if (in.getVersion() < Constants.TCP_PROTOCOL_VERSION_20) {
            in.readInt();
        }
        identity = in.readBoolean();
        nullable = in.readInt();
    }

    /**
     * Write a result column to the given output.
     *
     * @param out the object to where to write the data
     * @param result the result
     * @param i the column index
     * @throws IOException on failure
     */
    public static void writeColumn(DuckDBTransfer out, ResultInterface result, int i)
            throws IOException {
        out.writeString(result.getAlias(i));
        out.writeString(result.getSchemaName(i));
        out.writeString(result.getTableName(i));
        out.writeString(result.getColumnName(i));
        TypeInfo type = result.getColumnType(i);
        out.writeTypeInfo(type);
        if (out.getVersion() < Constants.TCP_PROTOCOL_VERSION_20) {
            out.writeInt(type.getDisplaySize());
        }
        out.writeBoolean(result.isIdentity(i));
        out.writeInt(result.getNullable(i));
    }

}
