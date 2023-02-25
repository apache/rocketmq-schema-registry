package org.apache.rocketmq.schema.registry.core.schema;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class SchemaReference implements Comparable<SchemaReference>{
	
	private String name;
	private String subject;
	private Integer version;
	
	public SchemaReference(@JsonProperty("name") String name,
							@JsonProperty("subject") String subject,
							@JsonProperty("version") Integer version ) {
		this.name = name;
		this.subject = subject;
		this.version = version;
	}
	
	@JsonProperty("name")
	public String getName() {
		return name;
	}
	
	@JsonProperty("name")
	public void setName(String name) {
		this.name = name;
	}
	
	@JsonProperty("subject")
	public String getSubject() {
		return subject;
	}
	
	@JsonProperty("subject")
	public void setSubject(String subject) {
		this.subject = subject;
	}
	
	@JsonProperty("version")
	public Integer getVersion() {
		return this.version;
	}
	
	@JsonProperty("version")
	public void setVersion(Integer version) {
		this.version = version;
	}
	
	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		SchemaReference that = (SchemaReference) o;
		return Objects.equals(name, that.name)
				&& Objects.equals(subject, that.subject)
				&& Objects.equals(version, that.version);
	}
	
	@Override
	public int hashCode() {
		return Objects.hash(name, subject, version);
	}
	
	@Override
	public int compareTo(SchemaReference that) {
		int result = this.subject.compareTo(that.subject);
		if (result != 0) {
			return result;
		}
		result = this.version - that.version;
		return result;
	}
	
	@Override
	public String toString() {
		return "{"
				+ "name='"
				+ name
				+ '\''
				+ ", subject='"
				+ subject
				+ '\''
				+ ", version="
				+ version
				+ '}';
	}
	
	
}