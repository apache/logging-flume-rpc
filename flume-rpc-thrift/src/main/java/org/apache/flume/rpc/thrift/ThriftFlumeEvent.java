/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flume.rpc.thrift;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import org.apache.thrift.TBase;
import org.apache.thrift.TBaseHelper;
import org.apache.thrift.TException;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.TFieldRequirementType;
import org.apache.thrift.annotation.Nullable;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.thrift.meta_data.FieldValueMetaData;
import org.apache.thrift.meta_data.MapMetaData;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TMap;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.protocol.TProtocolUtil;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.protocol.TType;
import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;
import org.apache.thrift.scheme.TupleScheme;
import org.apache.thrift.transport.TIOStreamTransport;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
public class ThriftFlumeEvent
        implements TBase<ThriftFlumeEvent, ThriftFlumeEvent._Fields>,
                Serializable,
                Cloneable,
                Comparable<ThriftFlumeEvent> {
    private static final TStruct STRUCT_DESC = new TStruct("ThriftFlumeEvent");

    private static final TField HEADERS_FIELD_DESC = new TField("headers", TType.MAP, (short) 1);
    private static final TField BODY_FIELD_DESC = new TField("body", TType.STRING, (short) 2);

    private static final SchemeFactory STANDARD_SCHEME_FACTORY = new ThriftFlumeEventStandardSchemeFactory();
    private static final SchemeFactory TUPLE_SCHEME_FACTORY = new ThriftFlumeEventTupleSchemeFactory();

    public @Nullable Map<java.lang.String, java.lang.String> headers; // required
    public @Nullable ByteBuffer body; // required

    /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
    public enum _Fields implements TFieldIdEnum {
        HEADERS((short) 1, "headers"),
        BODY((short) 2, "body");

        private static final Map<java.lang.String, _Fields> byName = new HashMap<java.lang.String, _Fields>();

        static {
            for (_Fields field : EnumSet.allOf(_Fields.class)) {
                byName.put(field.getFieldName(), field);
            }
        }

        /**
         * Find the _Fields constant that matches fieldId, or null if its not found.
         */
        @Nullable
        public static _Fields findByThriftId(int fieldId) {
            switch (fieldId) {
                case 1: // HEADERS
                    return HEADERS;
                case 2: // BODY
                    return BODY;
                default:
                    return null;
            }
        }

        /**
         * Find the _Fields constant that matches fieldId, throwing an exception
         * if it is not found.
         */
        public static _Fields findByThriftIdOrThrow(int fieldId) {
            _Fields fields = findByThriftId(fieldId);
            if (fields == null) throw new java.lang.IllegalArgumentException("Field " + fieldId + " doesn't exist!");
            return fields;
        }

        /**
         * Find the _Fields constant that matches name, or null if its not found.
         */
        @Nullable
        public static _Fields findByName(java.lang.String name) {
            return byName.get(name);
        }

        private final short _thriftId;
        private final java.lang.String _fieldName;

        _Fields(short thriftId, java.lang.String fieldName) {
            _thriftId = thriftId;
            _fieldName = fieldName;
        }

        @Override
        public short getThriftFieldId() {
            return _thriftId;
        }

        @Override
        public java.lang.String getFieldName() {
            return _fieldName;
        }
    }

    // isset id assignments
    public static final Map<_Fields, FieldMetaData> metaDataMap;

    static {
        Map<_Fields, FieldMetaData> tmpMap = new EnumMap<_Fields, FieldMetaData>(_Fields.class);
        tmpMap.put(
                _Fields.HEADERS,
                new FieldMetaData(
                        "headers",
                        TFieldRequirementType.REQUIRED,
                        new MapMetaData(
                                TType.MAP,
                                new FieldValueMetaData(TType.STRING),
                                new FieldValueMetaData(TType.STRING))));
        tmpMap.put(
                _Fields.BODY,
                new FieldMetaData("body", TFieldRequirementType.REQUIRED, new FieldValueMetaData(TType.STRING, true)));
        metaDataMap = Collections.unmodifiableMap(tmpMap);
        FieldMetaData.addStructMetaDataMap(ThriftFlumeEvent.class, metaDataMap);
    }

    public ThriftFlumeEvent() {}

    public ThriftFlumeEvent(Map<java.lang.String, java.lang.String> headers, ByteBuffer body) {
        this();
        this.headers = headers;
        this.body = TBaseHelper.copyBinary(body);
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public ThriftFlumeEvent(ThriftFlumeEvent other) {
        if (other.isSetHeaders()) {
            Map<java.lang.String, java.lang.String> __this__headers =
                    new HashMap<java.lang.String, java.lang.String>(other.headers);
            this.headers = __this__headers;
        }
        if (other.isSetBody()) {
            this.body = TBaseHelper.copyBinary(other.body);
        }
    }

    @Override
    public ThriftFlumeEvent deepCopy() {
        return new ThriftFlumeEvent(this);
    }

    @Override
    public void clear() {
        this.headers = null;
        this.body = null;
    }

    public int getHeadersSize() {
        return (this.headers == null) ? 0 : this.headers.size();
    }

    public void putToHeaders(java.lang.String key, java.lang.String val) {
        if (this.headers == null) {
            this.headers = new HashMap<java.lang.String, java.lang.String>();
        }
        this.headers.put(key, val);
    }

    @Nullable
    public Map<java.lang.String, java.lang.String> getHeaders() {
        return this.headers;
    }

    public ThriftFlumeEvent setHeaders(@Nullable Map<java.lang.String, java.lang.String> headers) {
        this.headers = headers;
        return this;
    }

    public void unsetHeaders() {
        this.headers = null;
    }

    /** Returns true if field headers is set (has been assigned a value) and false otherwise */
    public boolean isSetHeaders() {
        return this.headers != null;
    }

    public void setHeadersIsSet(boolean value) {
        if (!value) {
            this.headers = null;
        }
    }

    public byte[] getBody() {
        setBody(TBaseHelper.rightSize(body));
        return body == null ? null : body.array();
    }

    public ByteBuffer bufferForBody() {
        return TBaseHelper.copyBinary(body);
    }

    public ThriftFlumeEvent setBody(byte[] body) {
        this.body = body == null ? (ByteBuffer) null : ByteBuffer.wrap(body.clone());
        return this;
    }

    public ThriftFlumeEvent setBody(@Nullable ByteBuffer body) {
        this.body = TBaseHelper.copyBinary(body);
        return this;
    }

    public void unsetBody() {
        this.body = null;
    }

    /** Returns true if field body is set (has been assigned a value) and false otherwise */
    public boolean isSetBody() {
        return this.body != null;
    }

    public void setBodyIsSet(boolean value) {
        if (!value) {
            this.body = null;
        }
    }

    @Override
    public void setFieldValue(_Fields field, @Nullable java.lang.Object value) {
        switch (field) {
            case HEADERS:
                if (value == null) {
                    unsetHeaders();
                } else {
                    setHeaders((Map<java.lang.String, java.lang.String>) value);
                }
                break;

            case BODY:
                if (value == null) {
                    unsetBody();
                } else {
                    if (value instanceof byte[]) {
                        setBody((byte[]) value);
                    } else {
                        setBody((ByteBuffer) value);
                    }
                }
                break;
        }
    }

    @Nullable
    @Override
    public java.lang.Object getFieldValue(_Fields field) {
        switch (field) {
            case HEADERS:
                return getHeaders();

            case BODY:
                return getBody();
        }
        throw new java.lang.IllegalStateException();
    }

    /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
    @Override
    public boolean isSet(_Fields field) {
        if (field == null) {
            throw new java.lang.IllegalArgumentException();
        }

        switch (field) {
            case HEADERS:
                return isSetHeaders();
            case BODY:
                return isSetBody();
        }
        throw new java.lang.IllegalStateException();
    }

    @Override
    public boolean equals(java.lang.Object that) {
        if (that instanceof ThriftFlumeEvent) return this.equals((ThriftFlumeEvent) that);
        return false;
    }

    public boolean equals(ThriftFlumeEvent that) {
        if (that == null) return false;
        if (this == that) return true;

        boolean this_present_headers = true && this.isSetHeaders();
        boolean that_present_headers = true && that.isSetHeaders();
        if (this_present_headers || that_present_headers) {
            if (!(this_present_headers && that_present_headers)) return false;
            if (!this.headers.equals(that.headers)) return false;
        }

        boolean this_present_body = true && this.isSetBody();
        boolean that_present_body = true && that.isSetBody();
        if (this_present_body || that_present_body) {
            if (!(this_present_body && that_present_body)) return false;
            if (!this.body.equals(that.body)) return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int hashCode = 1;

        hashCode = hashCode * 8191 + ((isSetHeaders()) ? 131071 : 524287);
        if (isSetHeaders()) hashCode = hashCode * 8191 + headers.hashCode();

        hashCode = hashCode * 8191 + ((isSetBody()) ? 131071 : 524287);
        if (isSetBody()) hashCode = hashCode * 8191 + body.hashCode();

        return hashCode;
    }

    @Override
    public int compareTo(ThriftFlumeEvent other) {
        if (!getClass().equals(other.getClass())) {
            return getClass().getName().compareTo(other.getClass().getName());
        }

        int lastComparison = 0;

        lastComparison = java.lang.Boolean.compare(isSetHeaders(), other.isSetHeaders());
        if (lastComparison != 0) {
            return lastComparison;
        }
        if (isSetHeaders()) {
            lastComparison = TBaseHelper.compareTo(this.headers, other.headers);
            if (lastComparison != 0) {
                return lastComparison;
            }
        }
        lastComparison = java.lang.Boolean.compare(isSetBody(), other.isSetBody());
        if (lastComparison != 0) {
            return lastComparison;
        }
        if (isSetBody()) {
            lastComparison = TBaseHelper.compareTo(this.body, other.body);
            if (lastComparison != 0) {
                return lastComparison;
            }
        }
        return 0;
    }

    @Nullable
    @Override
    public _Fields fieldForId(int fieldId) {
        return _Fields.findByThriftId(fieldId);
    }

    @Override
    public void read(TProtocol iprot) throws TException {
        scheme(iprot).read(iprot, this);
    }

    @Override
    public void write(TProtocol oprot) throws TException {
        scheme(oprot).write(oprot, this);
    }

    @Override
    public java.lang.String toString() {
        java.lang.StringBuilder sb = new java.lang.StringBuilder("ThriftFlumeEvent(");
        boolean first = true;

        sb.append("headers:");
        if (this.headers == null) {
            sb.append("null");
        } else {
            sb.append(this.headers);
        }
        first = false;
        if (!first) sb.append(", ");
        sb.append("body:");
        if (this.body == null) {
            sb.append("null");
        } else {
            TBaseHelper.toString(this.body, sb);
        }
        first = false;
        sb.append(")");
        return sb.toString();
    }

    public void validate() throws TException {
        // check for required fields
        if (headers == null) {
            throw new TProtocolException("Required field 'headers' was not present! Struct: " + toString());
        }
        if (body == null) {
            throw new TProtocolException("Required field 'body' was not present! Struct: " + toString());
        }
        // check for sub-struct validity
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        try {
            write(new TCompactProtocol(new TIOStreamTransport(out)));
        } catch (TException te) {
            throw new IOException(te);
        }
    }

    private void readObject(ObjectInputStream in) throws IOException, java.lang.ClassNotFoundException {
        try {
            read(new TCompactProtocol(new TIOStreamTransport(in)));
        } catch (TException te) {
            throw new IOException(te);
        }
    }

    private static class ThriftFlumeEventStandardSchemeFactory implements SchemeFactory {
        @Override
        public ThriftFlumeEventStandardScheme getScheme() {
            return new ThriftFlumeEventStandardScheme();
        }
    }

    private static class ThriftFlumeEventStandardScheme extends StandardScheme<ThriftFlumeEvent> {

        @Override
        public void read(TProtocol iprot, ThriftFlumeEvent struct) throws TException {
            iprot.incrementRecursionDepth();
            try {
                TField schemeField;
                iprot.readStructBegin();
                while (true) {
                    schemeField = iprot.readFieldBegin();
                    if (schemeField.type == TType.STOP) {
                        break;
                    }
                    switch (schemeField.id) {
                        case 1: // HEADERS
                            if (schemeField.type == TType.MAP) {
                                {
                                    TMap _map0 = iprot.readMapBegin();
                                    struct.headers = new HashMap<java.lang.String, java.lang.String>(2 * _map0.size);
                                    @Nullable java.lang.String _key1;
                                    @Nullable java.lang.String _val2;
                                    for (int _i3 = 0; _i3 < _map0.size; ++_i3) {
                                        _key1 = iprot.readString();
                                        _val2 = iprot.readString();
                                        struct.headers.put(_key1, _val2);
                                    }
                                    iprot.readMapEnd();
                                }
                                struct.setHeadersIsSet(true);
                            } else {
                                TProtocolUtil.skip(iprot, schemeField.type);
                            }
                            break;
                        case 2: // BODY
                            if (schemeField.type == TType.STRING) {
                                struct.body = iprot.readBinary();
                                struct.setBodyIsSet(true);
                            } else {
                                TProtocolUtil.skip(iprot, schemeField.type);
                            }
                            break;
                        default:
                            TProtocolUtil.skip(iprot, schemeField.type);
                    }
                    iprot.readFieldEnd();
                }
                iprot.readStructEnd();

                // check for required fields of primitive type, which can't be checked in the validate method
                struct.validate();
            } finally {
                iprot.decrementRecursionDepth();
            }
        }

        @Override
        public void write(TProtocol oprot, ThriftFlumeEvent struct) throws TException {
            struct.validate();

            oprot.writeStructBegin(STRUCT_DESC);
            if (struct.headers != null) {
                oprot.writeFieldBegin(HEADERS_FIELD_DESC);
                {
                    oprot.writeMapBegin(new TMap(TType.STRING, TType.STRING, struct.headers.size()));
                    for (Map.Entry<java.lang.String, java.lang.String> _iter4 : struct.headers.entrySet()) {
                        oprot.writeString(_iter4.getKey());
                        oprot.writeString(_iter4.getValue());
                    }
                    oprot.writeMapEnd();
                }
                oprot.writeFieldEnd();
            }
            if (struct.body != null) {
                oprot.writeFieldBegin(BODY_FIELD_DESC);
                oprot.writeBinary(struct.body);
                oprot.writeFieldEnd();
            }
            oprot.writeFieldStop();
            oprot.writeStructEnd();
        }
    }

    private static class ThriftFlumeEventTupleSchemeFactory implements SchemeFactory {
        @Override
        public ThriftFlumeEventTupleScheme getScheme() {
            return new ThriftFlumeEventTupleScheme();
        }
    }

    private static class ThriftFlumeEventTupleScheme extends TupleScheme<ThriftFlumeEvent> {

        @Override
        public void write(TProtocol prot, ThriftFlumeEvent struct) throws TException {
            TTupleProtocol oprot = (TTupleProtocol) prot;
            {
                oprot.writeI32(struct.headers.size());
                for (Map.Entry<java.lang.String, java.lang.String> _iter5 : struct.headers.entrySet()) {
                    oprot.writeString(_iter5.getKey());
                    oprot.writeString(_iter5.getValue());
                }
            }
            oprot.writeBinary(struct.body);
        }

        @Override
        public void read(TProtocol prot, ThriftFlumeEvent struct) throws TException {
            prot.incrementRecursionDepth();
            try {
                TTupleProtocol iprot = (TTupleProtocol) prot;
                {
                    TMap _map6 = iprot.readMapBegin(TType.STRING, TType.STRING);
                    struct.headers = new HashMap<java.lang.String, java.lang.String>(2 * _map6.size);
                    @Nullable java.lang.String _key7;
                    @Nullable java.lang.String _val8;
                    for (int _i9 = 0; _i9 < _map6.size; ++_i9) {
                        _key7 = iprot.readString();
                        _val8 = iprot.readString();
                        struct.headers.put(_key7, _val8);
                    }
                }
                struct.setHeadersIsSet(true);
                struct.body = iprot.readBinary();
                struct.setBodyIsSet(true);
            } finally {
                prot.decrementRecursionDepth();
            }
        }
    }

    private static <S extends IScheme> S scheme(TProtocol proto) {
        return (StandardScheme.class.equals(proto.getScheme()) ? STANDARD_SCHEME_FACTORY : TUPLE_SCHEME_FACTORY)
                .getScheme();
    }
}
