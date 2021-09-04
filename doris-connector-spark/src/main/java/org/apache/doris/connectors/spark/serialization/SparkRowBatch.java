package org.apache.doris.connectors.spark.serialization;

import com.google.common.base.Preconditions;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.Types;
import org.apache.doris.connectors.base.exception.DorisException;
import org.apache.doris.connectors.base.rest.models.Schema;
import org.apache.doris.connectors.base.serialization.RowBatch;
import org.apache.doris.thrift.TScanBatchResult;
import org.apache.spark.sql.types.Decimal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;

public class SparkRowBatch extends RowBatch {
    private static final Logger LOG = LoggerFactory.getLogger(SparkRowBatch.class);

    public SparkRowBatch(TScanBatchResult nextResult, Schema schema) {
        super(nextResult, schema);
    }

    public void convertArrowToRowBatch() throws DorisException {
        try {
            for (int col = 0; col < fieldVectors.size(); col++) {
                FieldVector curFieldVector = fieldVectors.get(col);
                Types.MinorType mt = curFieldVector.getMinorType();

                final String currentType = schema.get(col).getType();
                switch (currentType) {
                    case "NULL_TYPE":
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            addValueToRow(rowIndex, null);
                        }
                        break;
                    case "BOOLEAN":
                        Preconditions.checkArgument(mt.equals(Types.MinorType.BIT),
                                typeMismatchMessage(currentType, mt));
                        BitVector bitVector = (BitVector) curFieldVector;
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            Object fieldValue = bitVector.isNull(rowIndex) ? null : bitVector.get(rowIndex) != 0;
                            addValueToRow(rowIndex, fieldValue);
                        }
                        break;
                    case "TINYINT":
                        Preconditions.checkArgument(mt.equals(Types.MinorType.TINYINT),
                                typeMismatchMessage(currentType, mt));
                        TinyIntVector tinyIntVector = (TinyIntVector) curFieldVector;
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            Object fieldValue = tinyIntVector.isNull(rowIndex) ? null : tinyIntVector.get(rowIndex);
                            addValueToRow(rowIndex, fieldValue);
                        }
                        break;
                    case "SMALLINT":
                        Preconditions.checkArgument(mt.equals(Types.MinorType.SMALLINT),
                                typeMismatchMessage(currentType, mt));
                        SmallIntVector smallIntVector = (SmallIntVector) curFieldVector;
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            Object fieldValue = smallIntVector.isNull(rowIndex) ? null : smallIntVector.get(rowIndex);
                            addValueToRow(rowIndex, fieldValue);
                        }
                        break;
                    case "INT":
                        Preconditions.checkArgument(mt.equals(Types.MinorType.INT),
                                typeMismatchMessage(currentType, mt));
                        IntVector intVector = (IntVector) curFieldVector;
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            Object fieldValue = intVector.isNull(rowIndex) ? null : intVector.get(rowIndex);
                            addValueToRow(rowIndex, fieldValue);
                        }
                        break;
                    case "BIGINT":
                        Preconditions.checkArgument(mt.equals(Types.MinorType.BIGINT),
                                typeMismatchMessage(currentType, mt));
                        BigIntVector bigIntVector = (BigIntVector) curFieldVector;
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            Object fieldValue = bigIntVector.isNull(rowIndex) ? null : bigIntVector.get(rowIndex);
                            addValueToRow(rowIndex, fieldValue);
                        }
                        break;
                    case "FLOAT":
                        Preconditions.checkArgument(mt.equals(Types.MinorType.FLOAT4),
                                typeMismatchMessage(currentType, mt));
                        Float4Vector float4Vector = (Float4Vector) curFieldVector;
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            Object fieldValue = float4Vector.isNull(rowIndex) ? null : float4Vector.get(rowIndex);
                            addValueToRow(rowIndex, fieldValue);
                        }
                        break;
                    case "TIME":
                    case "DOUBLE":
                        Preconditions.checkArgument(mt.equals(Types.MinorType.FLOAT8),
                                typeMismatchMessage(currentType, mt));
                        Float8Vector float8Vector = (Float8Vector) curFieldVector;
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            Object fieldValue = float8Vector.isNull(rowIndex) ? null : float8Vector.get(rowIndex);
                            addValueToRow(rowIndex, fieldValue);
                        }
                        break;
                    case "BINARY":
                        Preconditions.checkArgument(mt.equals(Types.MinorType.VARBINARY),
                                typeMismatchMessage(currentType, mt));
                        VarBinaryVector varBinaryVector = (VarBinaryVector) curFieldVector;
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            Object fieldValue = varBinaryVector.isNull(rowIndex) ? null : varBinaryVector.get(rowIndex);
                            addValueToRow(rowIndex, fieldValue);
                        }
                        break;
                    case "DECIMAL":
                        Preconditions.checkArgument(mt.equals(Types.MinorType.VARCHAR),
                                typeMismatchMessage(currentType, mt));
                        VarCharVector varCharVectorForDecimal = (VarCharVector) curFieldVector;
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            if (varCharVectorForDecimal.isNull(rowIndex)) {
                                addValueToRow(rowIndex, null);
                                continue;
                            }
                            String decimalValue = new String(varCharVectorForDecimal.get(rowIndex));
                            Decimal decimal = new Decimal();
                            try {
                                decimal.set(new scala.math.BigDecimal(new BigDecimal(decimalValue)));
                            } catch (NumberFormatException e) {
                                String errMsg = "Decimal response result '" + decimalValue + "' is illegal.";
                                LOG.error(errMsg, e);
                                throw new DorisException(errMsg);
                            }
                            addValueToRow(rowIndex, decimal);
                        }
                        break;
                    case "DECIMALV2":
                        Preconditions.checkArgument(mt.equals(Types.MinorType.DECIMAL),
                                typeMismatchMessage(currentType, mt));
                        DecimalVector decimalVector = (DecimalVector) curFieldVector;
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            if (decimalVector.isNull(rowIndex)) {
                                addValueToRow(rowIndex, null);
                                continue;
                            }
                            Decimal decimalV2 = Decimal.apply(decimalVector.getObject(rowIndex));
                            addValueToRow(rowIndex, decimalV2);
                        }
                        break;
                    case "DATE":
                    case "DATETIME":
                    case "LARGEINT":
                    case "CHAR":
                    case "VARCHAR":
                        Preconditions.checkArgument(mt.equals(Types.MinorType.VARCHAR),
                                typeMismatchMessage(currentType, mt));
                        VarCharVector varCharVector = (VarCharVector) curFieldVector;
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            if (varCharVector.isNull(rowIndex)) {
                                addValueToRow(rowIndex, null);
                                continue;
                            }
                            String value = new String(varCharVector.get(rowIndex), StandardCharsets.UTF_8);
                            addValueToRow(rowIndex, value);
                        }
                        break;
                    default:
                        String errMsg = "Unsupported type " + schema.get(col).getType();
                        LOG.error(errMsg);
                        throw new DorisException(errMsg);
                }
            }
        } catch (Exception e) {
            close();
            throw e;
        }
    }
}
