package org.apache.rocketmq.schema.registry.core.proto.dynamic;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.DescriptorProtos.EnumDescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumDescriptorProto.EnumReservedRange;
import com.google.protobuf.DescriptorProtos.EnumValueDescriptorProto;
import org.apache.rocketmq.schema.registry.core.proto.MetaProto;

import java.util.Map;

import static org.apache.rocketmq.schema.registry.core.proto.dynamic.DynamicSchema.toMeta;

/**
 * EnumDefinition
 */
public class EnumDefinition {
  // --- public static ---

  public static Builder newBuilder(String enumName) {
    return newBuilder(enumName, null, null);
  }

  public static Builder newBuilder(String enumName, Boolean allowAlias, Boolean isDeprecated) {
    return new Builder(enumName, allowAlias, isDeprecated);
  }

  // --- public ---

  public String toString() {
    return mEnumType.toString();
  }

  // --- package ---

  EnumDescriptorProto getEnumType() {
    return mEnumType;
  }

  // --- private ---

  private EnumDefinition(EnumDescriptorProto enumType) {
    mEnumType = enumType;
  }

  private EnumDescriptorProto mEnumType;

  /**
   * EnumDefinition.Builder
   */
  public static class Builder {
    // --- public ---

    public String getName() {
      return mEnumTypeBuilder.getName();
    }

    public Builder addValue(String name, int num) {
      return addValue(name, num, null, null, null);
    }

    // Note: added
    public Builder addValue(
        String name, int num, String doc, Map<String, String> params, Boolean isDeprecated) {
      EnumValueDescriptorProto.Builder enumValBuilder = EnumValueDescriptorProto.newBuilder();
      enumValBuilder.setName(name).setNumber(num);
      if (isDeprecated != null) {
        DescriptorProtos.EnumValueOptions.Builder optionsBuilder =
                DescriptorProtos.EnumValueOptions.newBuilder();
        optionsBuilder.setDeprecated(isDeprecated);
        enumValBuilder.mergeOptions(optionsBuilder.build());
      }
      MetaProto.Meta meta = toMeta(doc, params);
      if (meta != null) {
        DescriptorProtos.EnumValueOptions.Builder optionsBuilder =
                DescriptorProtos.EnumValueOptions.newBuilder();
        optionsBuilder.setExtension(MetaProto.enumValueMeta, meta);
        enumValBuilder.mergeOptions(optionsBuilder.build());
      }
      mEnumTypeBuilder.addValue(enumValBuilder.build());
      return this;
    }

    // Note: added
    public Builder addReservedName(String reservedName) {
      mEnumTypeBuilder.addReservedName(reservedName);
      return this;
    }

    // Note: added
    public Builder addReservedRange(int start, int end) {
      EnumReservedRange.Builder rangeBuilder = EnumReservedRange.newBuilder();
      rangeBuilder.setStart(start).setEnd(end);
      mEnumTypeBuilder.addReservedRange(rangeBuilder.build());
      return this;
    }

    // Note: added
    public Builder setMeta(String doc, Map<String, String> params) {
      MetaProto.Meta meta = toMeta(doc, params);
      if (meta != null) {
        DescriptorProtos.EnumOptions.Builder optionsBuilder =
                DescriptorProtos.EnumOptions.newBuilder();
        optionsBuilder.setExtension(MetaProto.enumMeta, meta);
        mEnumTypeBuilder.mergeOptions(optionsBuilder.build());
      }
      return this;
    }

    public EnumDefinition build() {
      return new EnumDefinition(mEnumTypeBuilder.build());
    }

    // --- private ---

    private Builder(String enumName, Boolean allowAlias, Boolean isDeprecated) {
      mEnumTypeBuilder = EnumDescriptorProto.newBuilder();
      mEnumTypeBuilder.setName(enumName);
      if (allowAlias != null) {
        DescriptorProtos.EnumOptions.Builder optionsBuilder =
            DescriptorProtos.EnumOptions.newBuilder();
        optionsBuilder.setAllowAlias(allowAlias);
        mEnumTypeBuilder.mergeOptions(optionsBuilder.build());
      }
      if (isDeprecated != null) {
        DescriptorProtos.EnumOptions.Builder optionsBuilder =
            DescriptorProtos.EnumOptions.newBuilder();
        optionsBuilder.setDeprecated(isDeprecated);
        mEnumTypeBuilder.mergeOptions(optionsBuilder.build());
      }
    }

    private EnumDescriptorProto.Builder mEnumTypeBuilder;
  }
}
