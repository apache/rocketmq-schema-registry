package org.apache.rocketmq.schema.registry.core.provider.diff;

import com.squareup.wire.schema.internal.parser.FieldElement;
import com.squareup.wire.schema.internal.parser.OneOfElement;
import org.apache.rocketmq.schema.registry.core.provider.Context;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.rocketmq.schema.registry.core.provider.Difference.Type.ONEOF_FIELD_ADDED;
import static org.apache.rocketmq.schema.registry.core.provider.Difference.Type.ONEOF_FIELD_REMOVED;

public class OneOfDiff {
  static void compare(final Context ctx, final OneOfElement original, final OneOfElement update) {
    Map<Integer, FieldElement> originalByTag = new HashMap<>();
    for (FieldElement field : original.getFields()) {
      originalByTag.put(field.getTag(), field);
    }
    Map<Integer, FieldElement> updateByTag = new HashMap<>();
    for (FieldElement field : update.getFields()) {
      updateByTag.put(field.getTag(), field);
    }

    Set<Integer> allTags = new HashSet<>(originalByTag.keySet());
    allTags.addAll(updateByTag.keySet());
    for (Integer tag : allTags) {
      try (Context.PathScope pathScope = ctx.enterPath(tag.toString())) {
        FieldElement originalField = originalByTag.get(tag);
        FieldElement updateField = updateByTag.get(tag);
        if (updateField == null) {
          ctx.addDifference(ONEOF_FIELD_REMOVED);
        } else if (originalField == null) {
          ctx.addDifference(ONEOF_FIELD_ADDED);
        } else {
          FieldSchemaDiff.compare(ctx, originalField, updateField);
        }
      }
    }
  }
}
