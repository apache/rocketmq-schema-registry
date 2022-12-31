package org.apache.rocketmq.schema.registry.core.proto;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.util.Map;

public class DecimalUtils {

  public static BigDecimal toBigDecimal(Decimal decimal) {
    MathContext mc = new MathContext(decimal.getPrecision());
    return new BigDecimal(
        new BigInteger(decimal.getValue().toByteArray()),
        decimal.getScale(),
        mc);
  }

  public static BigDecimal toBigDecimal(Message message) {
    byte[] decimalValue = new byte[0];
    Integer precision = null;
    int scale = 0;
    for (Map.Entry<FieldDescriptor, Object> entry : message.getAllFields().entrySet()) {
      if (entry.getKey().getName().equals("value")) {
        decimalValue = ((ByteString) entry.getValue()).toByteArray();
      } else if (entry.getKey().getName().equals("precision")) {
        precision = ((Number) entry.getValue()).intValue();
      } else if (entry.getKey().getName().equals("scale")) {
        scale = ((Number) entry.getValue()).intValue();
      }
    }
    if (precision != null) {
      MathContext mc = new MathContext(precision);
      return new BigDecimal(new BigInteger(decimalValue), scale, mc);
    } else {
      return new BigDecimal(new BigInteger(decimalValue), scale);
    }
  }

  public static Decimal fromBigDecimal(BigDecimal decimal) {
    return Decimal.newBuilder()
        .setValue(ByteString.copyFrom(decimal.unscaledValue().toByteArray()))
        .setPrecision(decimal.precision())
        .setScale(decimal.scale())
        .build();
  }
}