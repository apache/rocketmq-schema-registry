package org.apache.rocketmq.schema.registry.core.provider;

import java.util.Objects;

public class Difference {
	public enum Type {
		PACKAGE_CHANGED, MESSAGE_ADDED, MESSAGE_REMOVED, MESSAGE_MOVED,
		ENUM_ADDED, ENUM_REMOVED, ENUM_CONST_ADDED, ENUM_CONST_CHANGED,
		ENUM_CONST_REMOVED, FIELD_ADDED, FIELD_REMOVED, FIELD_NAME_CHANGED, FIELD_KIND_CHANGED,
		FIELD_SCALAR_KIND_CHANGED, FIELD_NAMED_TYPE_CHANGED,
		FIELD_NUMERIC_LABEL_CHANGED, FIELD_STRING_OR_BYTES_LABEL_CHANGED,
		REQUIRED_FIELD_ADDED, REQUIRED_FIELD_REMOVED,
		ONEOF_ADDED, ONEOF_REMOVED,
		ONEOF_FIELD_ADDED, ONEOF_FIELD_REMOVED, MULTIPLE_FIELDS_MOVED_TO_ONEOF,
	}
	
	private final String fullPath;
	private final Type type;
	
	public Difference(final Type type, final String fullPath) {
		this.fullPath = fullPath;
		this.type = type;
	}
	
	public String getFullPath() {
		return fullPath;
	}
	
	public Type getType() {
		return type;
	}
	
	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		Difference that = (Difference) o;
		return Objects.equals(fullPath, that.fullPath) && type == that.type;
	}
	
	@Override
	public int hashCode() {
		return Objects.hash(fullPath, type);
	}
	
	@Override
	public String toString() {
		return "Difference{" + "fullPath='" + fullPath + '\'' + ", type=" + type + '}';
	}
}
