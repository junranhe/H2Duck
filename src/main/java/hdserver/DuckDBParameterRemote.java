package hdserver;


import org.h2.api.ErrorCode;
import org.h2.expression.ParameterInterface;
import org.h2.message.DbException;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueLob;
import java.io.IOException;
import java.sql.ResultSetMetaData;

/**
 * A client side (remote) parameter.
 */
public class DuckDBParameterRemote implements ParameterInterface {

    private Value value;
    private final int index;
    private TypeInfo type = TypeInfo.TYPE_UNKNOWN;
    private int nullable = ResultSetMetaData.columnNullableUnknown;

    public DuckDBParameterRemote(int index) {
        this.index = index;
    }

    @Override
    public void setValue(Value newValue, boolean closeOld) {
        if (closeOld && value instanceof ValueLob) {
            ((ValueLob) value).remove();
        }
        value = newValue;
    }

    @Override
    public Value getParamValue() {
        return value;
    }

    @Override
    public void checkSet() {
        if (value == null) {
            throw DbException.get(ErrorCode.PARAMETER_NOT_SET_1, "#" + (index + 1));
        }
    }

    @Override
    public boolean isValueSet() {
        return value != null;
    }

    @Override
    public TypeInfo getType() {
        return value == null ? type : value.getType();
    }

    @Override
    public int getNullable() {
        return nullable;
    }

    /**
     * Read the parameter meta data from the transfer object.
     *
     * @param transfer the transfer object
     * @throws IOException on failure
     */
    public void readMetaData(DuckDBTransfer transfer) throws IOException {
        type = transfer.readTypeInfo();
        nullable = transfer.readInt();
    }

    /**
     * Write the parameter meta data to the transfer object.
     *
     * @param transfer the transfer object
     * @param p the parameter
     * @throws IOException on failure
     */
    public static void writeMetaData(DuckDBTransfer transfer, ParameterInterface p) throws IOException {
        transfer.writeTypeInfo(p.getType()).writeInt(p.getNullable());
    }


}

