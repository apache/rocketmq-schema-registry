/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.schema.registry.client.serde;
@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class Charge extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = -3449629867777645843L;
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Charge\",\"namespace\":\"org.apache.rocketmq.schema.registry.client.serde\",\"fields\":[{\"name\":\"item\",\"type\":\"string\"},{\"name\":\"amount\",\"type\":\"double\"}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }
  @Deprecated public CharSequence item;
  @Deprecated public double amount;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>. 
   */
  public Charge() {}

  /**
   * All-args constructor.
   */
  public Charge(CharSequence item, Double amount) {
    this.item = item;
    this.amount = amount;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call. 
  public Object get(int field$) {
    switch (field$) {
    case 0: return item;
    case 1: return amount;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call. 
  @SuppressWarnings(value="unchecked")
  public void put(int field$, Object value$) {
    switch (field$) {
    case 0: item = (CharSequence)value$; break;
    case 1: amount = (Double)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'item' field.
   */
  public CharSequence getItem() {
    return item;
  }

  /**
   * Sets the value of the 'item' field.
   * @param value the value to set.
   */
  public void setItem(CharSequence value) {
    this.item = value;
  }

  /**
   * Gets the value of the 'amount' field.
   */
  public Double getAmount() {
    return amount;
  }

  /**
   * Sets the value of the 'amount' field.
   * @param value the value to set.
   */
  public void setAmount(Double value) {
    this.amount = value;
  }

  /**
   * Creates a new Charge RecordBuilder.
   * @return A new Charge RecordBuilder
   */
  public static Builder newBuilder() {
    return new Builder();
  }
  
  /**
   * Creates a new Charge RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new Charge RecordBuilder
   */
  public static Builder newBuilder(Builder other) {
    return new Builder(other);
  }
  
  /**
   * Creates a new Charge RecordBuilder by copying an existing Charge instance.
   * @param other The existing instance to copy.
   * @return A new Charge RecordBuilder
   */
  public static Builder newBuilder(Charge other) {
    return new Builder(other);
  }
  
  /**
   * RecordBuilder for Charge instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<Charge>
    implements org.apache.avro.data.RecordBuilder<Charge> {

    private CharSequence item;
    private double amount;

    /** Creates a new Builder */
    private Builder() {
      super(Charge.SCHEMA$);
    }
    
    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.item)) {
        this.item = data().deepCopy(fields()[0].schema(), other.item);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.amount)) {
        this.amount = data().deepCopy(fields()[1].schema(), other.amount);
        fieldSetFlags()[1] = true;
      }
    }
    
    /**
     * Creates a Builder by copying an existing Charge instance
     * @param other The existing instance to copy.
     */
    private Builder(Charge other) {
            super(Charge.SCHEMA$);
      if (isValidValue(fields()[0], other.item)) {
        this.item = data().deepCopy(fields()[0].schema(), other.item);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.amount)) {
        this.amount = data().deepCopy(fields()[1].schema(), other.amount);
        fieldSetFlags()[1] = true;
      }
    }

    /**
      * Gets the value of the 'item' field.
      * @return The value.
      */
    public CharSequence getItem() {
      return item;
    }

    /**
      * Sets the value of the 'item' field.
      * @param value The value of 'item'.
      * @return This builder.
      */
    public Builder setItem(CharSequence value) {
      validate(fields()[0], value);
      this.item = value;
      fieldSetFlags()[0] = true;
      return this; 
    }

    /**
      * Checks whether the 'item' field has been set.
      * @return True if the 'item' field has been set, false otherwise.
      */
    public boolean hasItem() {
      return fieldSetFlags()[0];
    }


    /**
      * Clears the value of the 'item' field.
      * @return This builder.
      */
    public Builder clearItem() {
      item = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /**
      * Gets the value of the 'amount' field.
      * @return The value.
      */
    public Double getAmount() {
      return amount;
    }

    /**
      * Sets the value of the 'amount' field.
      * @param value The value of 'amount'.
      * @return This builder.
      */
    public Builder setAmount(double value) {
      validate(fields()[1], value);
      this.amount = value;
      fieldSetFlags()[1] = true;
      return this; 
    }

    /**
      * Checks whether the 'amount' field has been set.
      * @return True if the 'amount' field has been set, false otherwise.
      */
    public boolean hasAmount() {
      return fieldSetFlags()[1];
    }


    /**
      * Clears the value of the 'amount' field.
      * @return This builder.
      */
    public Builder clearAmount() {
      fieldSetFlags()[1] = false;
      return this;
    }

    @Override
    public Charge build() {
      try {
        Charge record = new Charge();
        record.item = fieldSetFlags()[0] ? this.item : (CharSequence) defaultValue(fields()[0]);
        record.amount = fieldSetFlags()[1] ? this.amount : (Double) defaultValue(fields()[1]);
        return record;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }

  private static final org.apache.avro.io.DatumWriter
    WRITER$ = new org.apache.avro.specific.SpecificDatumWriter(SCHEMA$);  

  @Override public void writeExternal(java.io.ObjectOutput out)
    throws java.io.IOException {
    WRITER$.write(this, org.apache.avro.specific.SpecificData.getEncoder(out));
  }

  private static final org.apache.avro.io.DatumReader
    READER$ = new org.apache.avro.specific.SpecificDatumReader(SCHEMA$);  

  @Override public void readExternal(java.io.ObjectInput in)
    throws java.io.IOException {
    READER$.read(this, org.apache.avro.specific.SpecificData.getDecoder(in));
  }

}
