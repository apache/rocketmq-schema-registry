package org.apache.rocketmq.schema.registry.core.provider.diff;

import com.squareup.wire.schema.internal.parser.EnumConstantElement;
import com.squareup.wire.schema.internal.parser.EnumElement;
import org.apache.rocketmq.schema.registry.core.provider.Context;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.rocketmq.schema.registry.core.provider.Difference.Type.*;

public class EnumSchemaDiff {
  static void compare(final Context ctx, final EnumElement original, final EnumElement update) {
    Map<Integer, EnumConstantElement> originalByTag = new HashMap<>();
    for (EnumConstantElement enumer : original.getConstants()) {
      originalByTag.put(enumer.getTag(), enumer);
    }
    Map<Integer, EnumConstantElement> updateByTag = new HashMap<>();
    for (EnumConstantElement enumer : update.getConstants()) {
      updateByTag.put(enumer.getTag(), enumer);
    }
    Set<Integer> allTags = new HashSet<>(originalByTag.keySet());
    allTags.addAll(updateByTag.keySet());

    for (Integer tag : allTags) {
      try (Context.PathScope pathScope = ctx.enterPath(tag.toString())) {
        EnumConstantElement originalEnumConstant = originalByTag.get(tag);
        EnumConstantElement updateEnumConstant = updateByTag.get(tag);
        if (updateEnumConstant == null) {
          ctx.addDifference(ENUM_CONST_REMOVED);
        } else if (originalEnumConstant == null) {
          ctx.addDifference(ENUM_CONST_ADDED);
        } else if (!originalEnumConstant.getName().equals(updateEnumConstant.getName())) {
          ctx.addDifference(ENUM_CONST_CHANGED);
        }
      }
    }
  }
}
