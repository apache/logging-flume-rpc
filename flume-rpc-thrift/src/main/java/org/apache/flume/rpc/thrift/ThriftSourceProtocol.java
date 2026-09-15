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
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.AsyncProcessFunction;
import org.apache.thrift.ProcessFunction;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TBase;
import org.apache.thrift.TBaseAsyncProcessor;
import org.apache.thrift.TBaseHelper;
import org.apache.thrift.TBaseProcessor;
import org.apache.thrift.TException;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.TFieldRequirementType;
import org.apache.thrift.TProcessor;
import org.apache.thrift.TSerializable;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.TServiceClientFactory;
import org.apache.thrift.annotation.Nullable;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.async.TAsyncClient;
import org.apache.thrift.async.TAsyncClientFactory;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.async.TAsyncMethodCall;
import org.apache.thrift.meta_data.EnumMetaData;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.thrift.meta_data.ListMetaData;
import org.apache.thrift.meta_data.StructMetaData;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TList;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.protocol.TProtocolUtil;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.protocol.TType;
import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;
import org.apache.thrift.scheme.TupleScheme;
import org.apache.thrift.server.AbstractNonblockingServer;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.apache.thrift.transport.TNonblockingTransport;
import org.apache.thrift.transport.TTransportException;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
public class ThriftSourceProtocol {

    public interface Iface {

        public Status append(ThriftFlumeEvent event) throws TException;

        public Status appendBatch(List<ThriftFlumeEvent> events) throws TException;
    }

    public interface AsyncIface {

        public void append(ThriftFlumeEvent event, AsyncMethodCallback<Status> resultHandler) throws TException;

        public void appendBatch(List<ThriftFlumeEvent> events, AsyncMethodCallback<Status> resultHandler)
                throws TException;
    }

    public static class Client extends TServiceClient implements Iface {
        public static class Factory implements TServiceClientFactory<Client> {
            public Factory() {}

            @Override
            public Client getClient(TProtocol prot) {
                return new Client(prot);
            }

            @Override
            public Client getClient(TProtocol iprot, TProtocol oprot) {
                return new Client(iprot, oprot);
            }
        }

        public Client(TProtocol prot) {
            super(prot, prot);
        }

        public Client(TProtocol iprot, TProtocol oprot) {
            super(iprot, oprot);
        }

        @Override
        public Status append(ThriftFlumeEvent event) throws TException {
            send_append(event);
            return recv_append();
        }

        public void send_append(ThriftFlumeEvent event) throws TException {
            append_args args = new append_args();
            args.setEvent(event);
            sendBase("append", args);
        }

        public Status recv_append() throws TException {
            append_result result = new append_result();
            receiveBase(result, "append");
            if (result.isSetSuccess()) {
                return result.success;
            }
            throw new TApplicationException(TApplicationException.MISSING_RESULT, "append failed: unknown result");
        }

        @Override
        public Status appendBatch(List<ThriftFlumeEvent> events) throws TException {
            send_appendBatch(events);
            return recv_appendBatch();
        }

        public void send_appendBatch(List<ThriftFlumeEvent> events) throws TException {
            appendBatch_args args = new appendBatch_args();
            args.setEvents(events);
            sendBase("appendBatch", args);
        }

        public Status recv_appendBatch() throws TException {
            appendBatch_result result = new appendBatch_result();
            receiveBase(result, "appendBatch");
            if (result.isSetSuccess()) {
                return result.success;
            }
            throw new TApplicationException(TApplicationException.MISSING_RESULT, "appendBatch failed: unknown result");
        }
    }

    public static class AsyncClient extends TAsyncClient implements AsyncIface {
        public static class Factory implements TAsyncClientFactory<AsyncClient> {
            private TAsyncClientManager clientManager;
            private TProtocolFactory protocolFactory;

            public Factory(TAsyncClientManager clientManager, TProtocolFactory protocolFactory) {
                this.clientManager = clientManager;
                this.protocolFactory = protocolFactory;
            }

            @Override
            public AsyncClient getAsyncClient(TNonblockingTransport transport) {
                return new AsyncClient(protocolFactory, clientManager, transport);
            }
        }

        public AsyncClient(
                TProtocolFactory protocolFactory, TAsyncClientManager clientManager, TNonblockingTransport transport) {
            super(protocolFactory, clientManager, transport);
        }

        @Override
        public void append(ThriftFlumeEvent event, AsyncMethodCallback<Status> resultHandler) throws TException {
            checkReady();
            append_call method_call = new append_call(event, resultHandler, this, ___protocolFactory, ___transport);
            this.___currentMethod = method_call;
            ___manager.call(method_call);
        }

        public static class append_call extends TAsyncMethodCall<Status> {
            private ThriftFlumeEvent event;

            public append_call(
                    ThriftFlumeEvent event,
                    AsyncMethodCallback<Status> resultHandler,
                    TAsyncClient client,
                    TProtocolFactory protocolFactory,
                    TNonblockingTransport transport)
                    throws TException {
                super(client, protocolFactory, transport, resultHandler, false);
                this.event = event;
            }

            @Override
            public void write_args(TProtocol prot) throws TException {
                prot.writeMessageBegin(new TMessage("append", TMessageType.CALL, 0));
                append_args args = new append_args();
                args.setEvent(event);
                args.write(prot);
                prot.writeMessageEnd();
            }

            @Override
            public Status getResult() throws TException {
                if (getState() != TAsyncMethodCall.State.RESPONSE_READ) {
                    throw new java.lang.IllegalStateException("Method call not finished!");
                }
                TMemoryInputTransport memoryTransport =
                        new TMemoryInputTransport(getFrameBuffer().array());
                TProtocol prot = client.getProtocolFactory().getProtocol(memoryTransport);
                return (new Client(prot)).recv_append();
            }
        }

        @Override
        public void appendBatch(List<ThriftFlumeEvent> events, AsyncMethodCallback<Status> resultHandler)
                throws TException {
            checkReady();
            appendBatch_call method_call =
                    new appendBatch_call(events, resultHandler, this, ___protocolFactory, ___transport);
            this.___currentMethod = method_call;
            ___manager.call(method_call);
        }

        public static class appendBatch_call extends TAsyncMethodCall<Status> {
            private List<ThriftFlumeEvent> events;

            public appendBatch_call(
                    List<ThriftFlumeEvent> events,
                    AsyncMethodCallback<Status> resultHandler,
                    TAsyncClient client,
                    TProtocolFactory protocolFactory,
                    TNonblockingTransport transport)
                    throws TException {
                super(client, protocolFactory, transport, resultHandler, false);
                this.events = events;
            }

            @Override
            public void write_args(TProtocol prot) throws TException {
                prot.writeMessageBegin(new TMessage("appendBatch", TMessageType.CALL, 0));
                appendBatch_args args = new appendBatch_args();
                args.setEvents(events);
                args.write(prot);
                prot.writeMessageEnd();
            }

            @Override
            public Status getResult() throws TException {
                if (getState() != TAsyncMethodCall.State.RESPONSE_READ) {
                    throw new java.lang.IllegalStateException("Method call not finished!");
                }
                TMemoryInputTransport memoryTransport =
                        new TMemoryInputTransport(getFrameBuffer().array());
                TProtocol prot = client.getProtocolFactory().getProtocol(memoryTransport);
                return (new Client(prot)).recv_appendBatch();
            }
        }
    }

    public static class Processor<I extends Iface> extends TBaseProcessor<I> implements TProcessor {
        private static final Logger _LOGGER = LogManager.getLogger(Processor.class.getName());

        public Processor(I iface) {
            super(
                    iface,
                    getProcessMap(
                            new HashMap<java.lang.String, ProcessFunction<I, ? extends TBase, ? extends TBase>>()));
        }

        protected Processor(
                I iface, Map<java.lang.String, ProcessFunction<I, ? extends TBase, ? extends TBase>> processMap) {
            super(iface, getProcessMap(processMap));
        }

        private static <I extends Iface>
                Map<java.lang.String, ProcessFunction<I, ? extends TBase, ? extends TBase>> getProcessMap(
                        Map<java.lang.String, ProcessFunction<I, ? extends TBase, ? extends TBase>> processMap) {
            processMap.put("append", new append());
            processMap.put("appendBatch", new appendBatch());
            return processMap;
        }

        public static class append<I extends Iface> extends ProcessFunction<I, append_args, append_result> {
            public append() {
                super("append");
            }

            @Override
            public append_args getEmptyArgsInstance() {
                return new append_args();
            }

            @Override
            public boolean isOneway() {
                return false;
            }

            @Override
            protected boolean rethrowUnhandledExceptions() {
                return false;
            }

            @Override
            public append_result getEmptyResultInstance() {
                return new append_result();
            }

            @Override
            public append_result getResult(I iface, append_args args) throws TException {
                append_result result = getEmptyResultInstance();
                result.success = iface.append(args.event);
                return result;
            }
        }

        public static class appendBatch<I extends Iface>
                extends ProcessFunction<I, appendBatch_args, appendBatch_result> {
            public appendBatch() {
                super("appendBatch");
            }

            @Override
            public appendBatch_args getEmptyArgsInstance() {
                return new appendBatch_args();
            }

            @Override
            public boolean isOneway() {
                return false;
            }

            @Override
            protected boolean rethrowUnhandledExceptions() {
                return false;
            }

            @Override
            public appendBatch_result getEmptyResultInstance() {
                return new appendBatch_result();
            }

            @Override
            public appendBatch_result getResult(I iface, appendBatch_args args) throws TException {
                appendBatch_result result = getEmptyResultInstance();
                result.success = iface.appendBatch(args.events);
                return result;
            }
        }
    }

    public static class AsyncProcessor<I extends AsyncIface> extends TBaseAsyncProcessor<I> {
        private static final Logger _LOGGER = LogManager.getLogger(AsyncProcessor.class.getName());

        public AsyncProcessor(I iface) {
            super(
                    iface,
                    getProcessMap(new HashMap<
                            java.lang.String, AsyncProcessFunction<I, ? extends TBase, ?, ? extends TBase>>()));
        }

        protected AsyncProcessor(
                I iface,
                Map<java.lang.String, AsyncProcessFunction<I, ? extends TBase, ?, ? extends TBase>> processMap) {
            super(iface, getProcessMap(processMap));
        }

        private static <I extends AsyncIface>
                Map<java.lang.String, AsyncProcessFunction<I, ? extends TBase, ?, ? extends TBase>> getProcessMap(
                        Map<java.lang.String, AsyncProcessFunction<I, ? extends TBase, ?, ? extends TBase>>
                                processMap) {
            processMap.put("append", new append());
            processMap.put("appendBatch", new appendBatch());
            return processMap;
        }

        public static class append<I extends AsyncIface>
                extends AsyncProcessFunction<I, append_args, Status, append_result> {
            public append() {
                super("append");
            }

            @Override
            public append_result getEmptyResultInstance() {
                return new append_result();
            }

            @Override
            public append_args getEmptyArgsInstance() {
                return new append_args();
            }

            @Override
            public AsyncMethodCallback<Status> getResultHandler(
                    final AbstractNonblockingServer.AsyncFrameBuffer fb, final int seqid) {
                final AsyncProcessFunction fcall = this;
                return new AsyncMethodCallback<Status>() {
                    @Override
                    public void onComplete(Status o) {
                        append_result result = new append_result();
                        result.success = o;
                        try {
                            fcall.sendResponse(fb, result, TMessageType.REPLY, seqid);
                        } catch (TTransportException e) {
                            _LOGGER.error("TTransportException writing to internal frame buffer", e);
                            fb.close();
                        } catch (java.lang.Exception e) {
                            _LOGGER.error("Exception writing to internal frame buffer", e);
                            onError(e);
                        }
                    }

                    @Override
                    public void onError(java.lang.Exception e) {
                        byte msgType = TMessageType.REPLY;
                        TSerializable msg;
                        append_result result = new append_result();
                        if (e instanceof TTransportException) {
                            _LOGGER.error("TTransportException inside handler", e);
                            fb.close();
                            return;
                        } else if (e instanceof TApplicationException) {
                            _LOGGER.error("TApplicationException inside handler", e);
                            msgType = TMessageType.EXCEPTION;
                            msg = (TApplicationException) e;
                        } else {
                            _LOGGER.error("Exception inside handler", e);
                            msgType = TMessageType.EXCEPTION;
                            msg = new TApplicationException(TApplicationException.INTERNAL_ERROR, e.getMessage());
                        }
                        try {
                            fcall.sendResponse(fb, msg, msgType, seqid);
                        } catch (java.lang.Exception ex) {
                            _LOGGER.error("Exception writing to internal frame buffer", ex);
                            fb.close();
                        }
                    }
                };
            }

            @Override
            public boolean isOneway() {
                return false;
            }

            @Override
            public void start(I iface, append_args args, AsyncMethodCallback<Status> resultHandler) throws TException {
                iface.append(args.event, resultHandler);
            }
        }

        public static class appendBatch<I extends AsyncIface>
                extends AsyncProcessFunction<I, appendBatch_args, Status, appendBatch_result> {
            public appendBatch() {
                super("appendBatch");
            }

            @Override
            public appendBatch_result getEmptyResultInstance() {
                return new appendBatch_result();
            }

            @Override
            public appendBatch_args getEmptyArgsInstance() {
                return new appendBatch_args();
            }

            @Override
            public AsyncMethodCallback<Status> getResultHandler(
                    final AbstractNonblockingServer.AsyncFrameBuffer fb, final int seqid) {
                final AsyncProcessFunction fcall = this;
                return new AsyncMethodCallback<Status>() {
                    @Override
                    public void onComplete(Status o) {
                        appendBatch_result result = new appendBatch_result();
                        result.success = o;
                        try {
                            fcall.sendResponse(fb, result, TMessageType.REPLY, seqid);
                        } catch (TTransportException e) {
                            _LOGGER.error("TTransportException writing to internal frame buffer", e);
                            fb.close();
                        } catch (java.lang.Exception e) {
                            _LOGGER.error("Exception writing to internal frame buffer", e);
                            onError(e);
                        }
                    }

                    @Override
                    public void onError(java.lang.Exception e) {
                        byte msgType = TMessageType.REPLY;
                        TSerializable msg;
                        appendBatch_result result = new appendBatch_result();
                        if (e instanceof TTransportException) {
                            _LOGGER.error("TTransportException inside handler", e);
                            fb.close();
                            return;
                        } else if (e instanceof TApplicationException) {
                            _LOGGER.error("TApplicationException inside handler", e);
                            msgType = TMessageType.EXCEPTION;
                            msg = (TApplicationException) e;
                        } else {
                            _LOGGER.error("Exception inside handler", e);
                            msgType = TMessageType.EXCEPTION;
                            msg = new TApplicationException(TApplicationException.INTERNAL_ERROR, e.getMessage());
                        }
                        try {
                            fcall.sendResponse(fb, msg, msgType, seqid);
                        } catch (java.lang.Exception ex) {
                            _LOGGER.error("Exception writing to internal frame buffer", ex);
                            fb.close();
                        }
                    }
                };
            }

            @Override
            public boolean isOneway() {
                return false;
            }

            @Override
            public void start(I iface, appendBatch_args args, AsyncMethodCallback<Status> resultHandler)
                    throws TException {
                iface.appendBatch(args.events, resultHandler);
            }
        }
    }

    @SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
    public static class append_args
            implements TBase<append_args, append_args._Fields>, Serializable, Cloneable, Comparable<append_args> {
        private static final TStruct STRUCT_DESC = new TStruct("append_args");

        private static final TField EVENT_FIELD_DESC = new TField("event", TType.STRUCT, (short) 1);

        private static final SchemeFactory STANDARD_SCHEME_FACTORY = new append_argsStandardSchemeFactory();
        private static final SchemeFactory TUPLE_SCHEME_FACTORY = new append_argsTupleSchemeFactory();

        public @Nullable ThriftFlumeEvent event; // required

        /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
        public enum _Fields implements TFieldIdEnum {
            EVENT((short) 1, "event");

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
                    case 1: // EVENT
                        return EVENT;
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
                if (fields == null)
                    throw new java.lang.IllegalArgumentException("Field " + fieldId + " doesn't exist!");
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
                    _Fields.EVENT,
                    new FieldMetaData(
                            "event",
                            TFieldRequirementType.DEFAULT,
                            new StructMetaData(TType.STRUCT, ThriftFlumeEvent.class)));
            metaDataMap = Collections.unmodifiableMap(tmpMap);
            FieldMetaData.addStructMetaDataMap(append_args.class, metaDataMap);
        }

        public append_args() {}

        public append_args(ThriftFlumeEvent event) {
            this();
            this.event = event;
        }

        /**
         * Performs a deep copy on <i>other</i>.
         */
        public append_args(append_args other) {
            if (other.isSetEvent()) {
                this.event = new ThriftFlumeEvent(other.event);
            }
        }

        @Override
        public append_args deepCopy() {
            return new append_args(this);
        }

        @Override
        public void clear() {
            this.event = null;
        }

        @Nullable
        public ThriftFlumeEvent getEvent() {
            return this.event;
        }

        public append_args setEvent(@Nullable ThriftFlumeEvent event) {
            this.event = event;
            return this;
        }

        public void unsetEvent() {
            this.event = null;
        }

        /** Returns true if field event is set (has been assigned a value) and false otherwise */
        public boolean isSetEvent() {
            return this.event != null;
        }

        public void setEventIsSet(boolean value) {
            if (!value) {
                this.event = null;
            }
        }

        @Override
        public void setFieldValue(_Fields field, @Nullable java.lang.Object value) {
            switch (field) {
                case EVENT:
                    if (value == null) {
                        unsetEvent();
                    } else {
                        setEvent((ThriftFlumeEvent) value);
                    }
                    break;
            }
        }

        @Nullable
        @Override
        public java.lang.Object getFieldValue(_Fields field) {
            switch (field) {
                case EVENT:
                    return getEvent();
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
                case EVENT:
                    return isSetEvent();
            }
            throw new java.lang.IllegalStateException();
        }

        @Override
        public boolean equals(java.lang.Object that) {
            if (that instanceof append_args) return this.equals((append_args) that);
            return false;
        }

        public boolean equals(append_args that) {
            if (that == null) return false;
            if (this == that) return true;

            boolean this_present_event = true && this.isSetEvent();
            boolean that_present_event = true && that.isSetEvent();
            if (this_present_event || that_present_event) {
                if (!(this_present_event && that_present_event)) return false;
                if (!this.event.equals(that.event)) return false;
            }

            return true;
        }

        @Override
        public int hashCode() {
            int hashCode = 1;

            hashCode = hashCode * 8191 + ((isSetEvent()) ? 131071 : 524287);
            if (isSetEvent()) hashCode = hashCode * 8191 + event.hashCode();

            return hashCode;
        }

        @Override
        public int compareTo(append_args other) {
            if (!getClass().equals(other.getClass())) {
                return getClass().getName().compareTo(other.getClass().getName());
            }

            int lastComparison = 0;

            lastComparison = java.lang.Boolean.compare(isSetEvent(), other.isSetEvent());
            if (lastComparison != 0) {
                return lastComparison;
            }
            if (isSetEvent()) {
                lastComparison = TBaseHelper.compareTo(this.event, other.event);
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
            java.lang.StringBuilder sb = new java.lang.StringBuilder("append_args(");
            boolean first = true;

            sb.append("event:");
            if (this.event == null) {
                sb.append("null");
            } else {
                sb.append(this.event);
            }
            first = false;
            sb.append(")");
            return sb.toString();
        }

        public void validate() throws TException {
            // check for required fields
            // check for sub-struct validity
            if (event != null) {
                event.validate();
            }
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

        private static class append_argsStandardSchemeFactory implements SchemeFactory {
            @Override
            public append_argsStandardScheme getScheme() {
                return new append_argsStandardScheme();
            }
        }

        private static class append_argsStandardScheme extends StandardScheme<append_args> {

            @Override
            public void read(TProtocol iprot, append_args struct) throws TException {
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
                            case 1: // EVENT
                                if (schemeField.type == TType.STRUCT) {
                                    struct.event = new ThriftFlumeEvent();
                                    struct.event.read(iprot);
                                    struct.setEventIsSet(true);
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
            public void write(TProtocol oprot, append_args struct) throws TException {
                struct.validate();

                oprot.writeStructBegin(STRUCT_DESC);
                if (struct.event != null) {
                    oprot.writeFieldBegin(EVENT_FIELD_DESC);
                    struct.event.write(oprot);
                    oprot.writeFieldEnd();
                }
                oprot.writeFieldStop();
                oprot.writeStructEnd();
            }
        }

        private static class append_argsTupleSchemeFactory implements SchemeFactory {
            @Override
            public append_argsTupleScheme getScheme() {
                return new append_argsTupleScheme();
            }
        }

        private static class append_argsTupleScheme extends TupleScheme<append_args> {

            @Override
            public void write(TProtocol prot, append_args struct) throws TException {
                TTupleProtocol oprot = (TTupleProtocol) prot;
                BitSet optionals = new BitSet();
                if (struct.isSetEvent()) {
                    optionals.set(0);
                }
                oprot.writeBitSet(optionals, 1);
                if (struct.isSetEvent()) {
                    struct.event.write(oprot);
                }
            }

            @Override
            public void read(TProtocol prot, append_args struct) throws TException {
                prot.incrementRecursionDepth();
                try {
                    TTupleProtocol iprot = (TTupleProtocol) prot;
                    BitSet incoming = iprot.readBitSet(1);
                    if (incoming.get(0)) {
                        struct.event = new ThriftFlumeEvent();
                        struct.event.read(iprot);
                        struct.setEventIsSet(true);
                    }
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

    @SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
    public static class append_result
            implements TBase<append_result, append_result._Fields>, Serializable, Cloneable, Comparable<append_result> {
        private static final TStruct STRUCT_DESC = new TStruct("append_result");

        private static final TField SUCCESS_FIELD_DESC = new TField("success", TType.I32, (short) 0);

        private static final SchemeFactory STANDARD_SCHEME_FACTORY = new append_resultStandardSchemeFactory();
        private static final SchemeFactory TUPLE_SCHEME_FACTORY = new append_resultTupleSchemeFactory();

        /**
         *
         * @see Status
         */
        public @Nullable Status success; // required

        /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
        public enum _Fields implements TFieldIdEnum {
            /**
             *
             * @see Status
             */
            SUCCESS((short) 0, "success");

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
                    case 0: // SUCCESS
                        return SUCCESS;
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
                if (fields == null)
                    throw new java.lang.IllegalArgumentException("Field " + fieldId + " doesn't exist!");
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
                    _Fields.SUCCESS,
                    new FieldMetaData(
                            "success", TFieldRequirementType.DEFAULT, new EnumMetaData(TType.ENUM, Status.class)));
            metaDataMap = Collections.unmodifiableMap(tmpMap);
            FieldMetaData.addStructMetaDataMap(append_result.class, metaDataMap);
        }

        public append_result() {}

        public append_result(Status success) {
            this();
            this.success = success;
        }

        /**
         * Performs a deep copy on <i>other</i>.
         */
        public append_result(append_result other) {
            if (other.isSetSuccess()) {
                this.success = other.success;
            }
        }

        @Override
        public append_result deepCopy() {
            return new append_result(this);
        }

        @Override
        public void clear() {
            this.success = null;
        }

        /**
         *
         * @see Status
         */
        @Nullable
        public Status getSuccess() {
            return this.success;
        }

        /**
         *
         * @see Status
         */
        public append_result setSuccess(@Nullable Status success) {
            this.success = success;
            return this;
        }

        public void unsetSuccess() {
            this.success = null;
        }

        /** Returns true if field success is set (has been assigned a value) and false otherwise */
        public boolean isSetSuccess() {
            return this.success != null;
        }

        public void setSuccessIsSet(boolean value) {
            if (!value) {
                this.success = null;
            }
        }

        @Override
        public void setFieldValue(_Fields field, @Nullable java.lang.Object value) {
            switch (field) {
                case SUCCESS:
                    if (value == null) {
                        unsetSuccess();
                    } else {
                        setSuccess((Status) value);
                    }
                    break;
            }
        }

        @Nullable
        @Override
        public java.lang.Object getFieldValue(_Fields field) {
            switch (field) {
                case SUCCESS:
                    return getSuccess();
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
                case SUCCESS:
                    return isSetSuccess();
            }
            throw new java.lang.IllegalStateException();
        }

        @Override
        public boolean equals(java.lang.Object that) {
            if (that instanceof append_result) return this.equals((append_result) that);
            return false;
        }

        public boolean equals(append_result that) {
            if (that == null) return false;
            if (this == that) return true;

            boolean this_present_success = true && this.isSetSuccess();
            boolean that_present_success = true && that.isSetSuccess();
            if (this_present_success || that_present_success) {
                if (!(this_present_success && that_present_success)) return false;
                if (!this.success.equals(that.success)) return false;
            }

            return true;
        }

        @Override
        public int hashCode() {
            int hashCode = 1;

            hashCode = hashCode * 8191 + ((isSetSuccess()) ? 131071 : 524287);
            if (isSetSuccess()) hashCode = hashCode * 8191 + success.getValue();

            return hashCode;
        }

        @Override
        public int compareTo(append_result other) {
            if (!getClass().equals(other.getClass())) {
                return getClass().getName().compareTo(other.getClass().getName());
            }

            int lastComparison = 0;

            lastComparison = java.lang.Boolean.compare(isSetSuccess(), other.isSetSuccess());
            if (lastComparison != 0) {
                return lastComparison;
            }
            if (isSetSuccess()) {
                lastComparison = TBaseHelper.compareTo(this.success, other.success);
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

        public void write(TProtocol oprot) throws TException {
            scheme(oprot).write(oprot, this);
        }

        @Override
        public java.lang.String toString() {
            java.lang.StringBuilder sb = new java.lang.StringBuilder("append_result(");
            boolean first = true;

            sb.append("success:");
            if (this.success == null) {
                sb.append("null");
            } else {
                sb.append(this.success);
            }
            first = false;
            sb.append(")");
            return sb.toString();
        }

        public void validate() throws TException {
            // check for required fields
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

        private static class append_resultStandardSchemeFactory implements SchemeFactory {
            @Override
            public append_resultStandardScheme getScheme() {
                return new append_resultStandardScheme();
            }
        }

        private static class append_resultStandardScheme extends StandardScheme<append_result> {

            @Override
            public void read(TProtocol iprot, append_result struct) throws TException {
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
                            case 0: // SUCCESS
                                if (schemeField.type == TType.I32) {
                                    struct.success = Status.findByValue(iprot.readI32());
                                    struct.setSuccessIsSet(true);
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
            public void write(TProtocol oprot, append_result struct) throws TException {
                struct.validate();

                oprot.writeStructBegin(STRUCT_DESC);
                if (struct.success != null) {
                    oprot.writeFieldBegin(SUCCESS_FIELD_DESC);
                    oprot.writeI32(struct.success.getValue());
                    oprot.writeFieldEnd();
                }
                oprot.writeFieldStop();
                oprot.writeStructEnd();
            }
        }

        private static class append_resultTupleSchemeFactory implements SchemeFactory {
            @Override
            public append_resultTupleScheme getScheme() {
                return new append_resultTupleScheme();
            }
        }

        private static class append_resultTupleScheme extends TupleScheme<append_result> {

            @Override
            public void write(TProtocol prot, append_result struct) throws TException {
                TTupleProtocol oprot = (TTupleProtocol) prot;
                BitSet optionals = new BitSet();
                if (struct.isSetSuccess()) {
                    optionals.set(0);
                }
                oprot.writeBitSet(optionals, 1);
                if (struct.isSetSuccess()) {
                    oprot.writeI32(struct.success.getValue());
                }
            }

            @Override
            public void read(TProtocol prot, append_result struct) throws TException {
                prot.incrementRecursionDepth();
                try {
                    TTupleProtocol iprot = (TTupleProtocol) prot;
                    BitSet incoming = iprot.readBitSet(1);
                    if (incoming.get(0)) {
                        struct.success = Status.findByValue(iprot.readI32());
                        struct.setSuccessIsSet(true);
                    }
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

    @SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
    public static class appendBatch_args
            implements TBase<appendBatch_args, appendBatch_args._Fields>,
                    Serializable,
                    Cloneable,
                    Comparable<appendBatch_args> {
        private static final TStruct STRUCT_DESC = new TStruct("appendBatch_args");

        private static final TField EVENTS_FIELD_DESC = new TField("events", TType.LIST, (short) 1);

        private static final SchemeFactory STANDARD_SCHEME_FACTORY = new appendBatch_argsStandardSchemeFactory();
        private static final SchemeFactory TUPLE_SCHEME_FACTORY = new appendBatch_argsTupleSchemeFactory();

        public @Nullable List<ThriftFlumeEvent> events; // required

        /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
        public enum _Fields implements TFieldIdEnum {
            EVENTS((short) 1, "events");

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
                    case 1: // EVENTS
                        return EVENTS;
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
                if (fields == null)
                    throw new java.lang.IllegalArgumentException("Field " + fieldId + " doesn't exist!");
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
                    _Fields.EVENTS,
                    new FieldMetaData(
                            "events",
                            TFieldRequirementType.DEFAULT,
                            new ListMetaData(TType.LIST, new StructMetaData(TType.STRUCT, ThriftFlumeEvent.class))));
            metaDataMap = Collections.unmodifiableMap(tmpMap);
            FieldMetaData.addStructMetaDataMap(appendBatch_args.class, metaDataMap);
        }

        public appendBatch_args() {}

        public appendBatch_args(List<ThriftFlumeEvent> events) {
            this();
            this.events = events;
        }

        /**
         * Performs a deep copy on <i>other</i>.
         */
        public appendBatch_args(appendBatch_args other) {
            if (other.isSetEvents()) {
                List<ThriftFlumeEvent> __this__events = new ArrayList<ThriftFlumeEvent>(other.events.size());
                for (ThriftFlumeEvent other_element : other.events) {
                    __this__events.add(new ThriftFlumeEvent(other_element));
                }
                this.events = __this__events;
            }
        }

        @Override
        public appendBatch_args deepCopy() {
            return new appendBatch_args(this);
        }

        @Override
        public void clear() {
            this.events = null;
        }

        public int getEventsSize() {
            return (this.events == null) ? 0 : this.events.size();
        }

        @Nullable
        public Iterator<ThriftFlumeEvent> getEventsIterator() {
            return (this.events == null) ? null : this.events.iterator();
        }

        public void addToEvents(ThriftFlumeEvent elem) {
            if (this.events == null) {
                this.events = new ArrayList<ThriftFlumeEvent>();
            }
            this.events.add(elem);
        }

        @Nullable
        public List<ThriftFlumeEvent> getEvents() {
            return this.events;
        }

        public appendBatch_args setEvents(@Nullable List<ThriftFlumeEvent> events) {
            this.events = events;
            return this;
        }

        public void unsetEvents() {
            this.events = null;
        }

        /** Returns true if field events is set (has been assigned a value) and false otherwise */
        public boolean isSetEvents() {
            return this.events != null;
        }

        public void setEventsIsSet(boolean value) {
            if (!value) {
                this.events = null;
            }
        }

        @Override
        public void setFieldValue(_Fields field, @Nullable java.lang.Object value) {
            switch (field) {
                case EVENTS:
                    if (value == null) {
                        unsetEvents();
                    } else {
                        setEvents((List<ThriftFlumeEvent>) value);
                    }
                    break;
            }
        }

        @Nullable
        @Override
        public java.lang.Object getFieldValue(_Fields field) {
            switch (field) {
                case EVENTS:
                    return getEvents();
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
                case EVENTS:
                    return isSetEvents();
            }
            throw new java.lang.IllegalStateException();
        }

        @Override
        public boolean equals(java.lang.Object that) {
            if (that instanceof appendBatch_args) return this.equals((appendBatch_args) that);
            return false;
        }

        public boolean equals(appendBatch_args that) {
            if (that == null) return false;
            if (this == that) return true;

            boolean this_present_events = true && this.isSetEvents();
            boolean that_present_events = true && that.isSetEvents();
            if (this_present_events || that_present_events) {
                if (!(this_present_events && that_present_events)) return false;
                if (!this.events.equals(that.events)) return false;
            }

            return true;
        }

        @Override
        public int hashCode() {
            int hashCode = 1;

            hashCode = hashCode * 8191 + ((isSetEvents()) ? 131071 : 524287);
            if (isSetEvents()) hashCode = hashCode * 8191 + events.hashCode();

            return hashCode;
        }

        @Override
        public int compareTo(appendBatch_args other) {
            if (!getClass().equals(other.getClass())) {
                return getClass().getName().compareTo(other.getClass().getName());
            }

            int lastComparison = 0;

            lastComparison = java.lang.Boolean.compare(isSetEvents(), other.isSetEvents());
            if (lastComparison != 0) {
                return lastComparison;
            }
            if (isSetEvents()) {
                lastComparison = TBaseHelper.compareTo(this.events, other.events);
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
            java.lang.StringBuilder sb = new java.lang.StringBuilder("appendBatch_args(");
            boolean first = true;

            sb.append("events:");
            if (this.events == null) {
                sb.append("null");
            } else {
                sb.append(this.events);
            }
            first = false;
            sb.append(")");
            return sb.toString();
        }

        public void validate() throws TException {
            // check for required fields
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

        private static class appendBatch_argsStandardSchemeFactory implements SchemeFactory {
            @Override
            public appendBatch_argsStandardScheme getScheme() {
                return new appendBatch_argsStandardScheme();
            }
        }

        private static class appendBatch_argsStandardScheme extends StandardScheme<appendBatch_args> {

            @Override
            public void read(TProtocol iprot, appendBatch_args struct) throws TException {
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
                            case 1: // EVENTS
                                if (schemeField.type == TType.LIST) {
                                    {
                                        TList _list10 = iprot.readListBegin();
                                        struct.events = new ArrayList<ThriftFlumeEvent>(_list10.size);
                                        @Nullable ThriftFlumeEvent _elem11;
                                        for (int _i12 = 0; _i12 < _list10.size; ++_i12) {
                                            _elem11 = new ThriftFlumeEvent();
                                            _elem11.read(iprot);
                                            struct.events.add(_elem11);
                                        }
                                        iprot.readListEnd();
                                    }
                                    struct.setEventsIsSet(true);
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
            public void write(TProtocol oprot, appendBatch_args struct) throws TException {
                struct.validate();

                oprot.writeStructBegin(STRUCT_DESC);
                if (struct.events != null) {
                    oprot.writeFieldBegin(EVENTS_FIELD_DESC);
                    {
                        oprot.writeListBegin(new TList(TType.STRUCT, struct.events.size()));
                        for (ThriftFlumeEvent _iter13 : struct.events) {
                            _iter13.write(oprot);
                        }
                        oprot.writeListEnd();
                    }
                    oprot.writeFieldEnd();
                }
                oprot.writeFieldStop();
                oprot.writeStructEnd();
            }
        }

        private static class appendBatch_argsTupleSchemeFactory implements SchemeFactory {
            @Override
            public appendBatch_argsTupleScheme getScheme() {
                return new appendBatch_argsTupleScheme();
            }
        }

        private static class appendBatch_argsTupleScheme extends TupleScheme<appendBatch_args> {

            @Override
            public void write(TProtocol prot, appendBatch_args struct) throws TException {
                TTupleProtocol oprot = (TTupleProtocol) prot;
                BitSet optionals = new BitSet();
                if (struct.isSetEvents()) {
                    optionals.set(0);
                }
                oprot.writeBitSet(optionals, 1);
                if (struct.isSetEvents()) {
                    {
                        oprot.writeI32(struct.events.size());
                        for (ThriftFlumeEvent _iter14 : struct.events) {
                            _iter14.write(oprot);
                        }
                    }
                }
            }

            @Override
            public void read(TProtocol prot, appendBatch_args struct) throws TException {
                prot.incrementRecursionDepth();
                try {
                    TTupleProtocol iprot = (TTupleProtocol) prot;
                    BitSet incoming = iprot.readBitSet(1);
                    if (incoming.get(0)) {
                        {
                            TList _list15 = iprot.readListBegin(TType.STRUCT);
                            struct.events = new ArrayList<ThriftFlumeEvent>(_list15.size);
                            @Nullable ThriftFlumeEvent _elem16;
                            for (int _i17 = 0; _i17 < _list15.size; ++_i17) {
                                _elem16 = new ThriftFlumeEvent();
                                _elem16.read(iprot);
                                struct.events.add(_elem16);
                            }
                        }
                        struct.setEventsIsSet(true);
                    }
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

    @SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
    public static class appendBatch_result
            implements TBase<appendBatch_result, appendBatch_result._Fields>,
                    Serializable,
                    Cloneable,
                    Comparable<appendBatch_result> {
        private static final TStruct STRUCT_DESC = new TStruct("appendBatch_result");

        private static final TField SUCCESS_FIELD_DESC = new TField("success", TType.I32, (short) 0);

        private static final SchemeFactory STANDARD_SCHEME_FACTORY = new appendBatch_resultStandardSchemeFactory();
        private static final SchemeFactory TUPLE_SCHEME_FACTORY = new appendBatch_resultTupleSchemeFactory();

        /**
         *
         * @see Status
         */
        public @Nullable Status success; // required

        /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
        public enum _Fields implements TFieldIdEnum {
            /**
             *
             * @see Status
             */
            SUCCESS((short) 0, "success");

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
                    case 0: // SUCCESS
                        return SUCCESS;
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
                if (fields == null)
                    throw new java.lang.IllegalArgumentException("Field " + fieldId + " doesn't exist!");
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
                    _Fields.SUCCESS,
                    new FieldMetaData(
                            "success", TFieldRequirementType.DEFAULT, new EnumMetaData(TType.ENUM, Status.class)));
            metaDataMap = Collections.unmodifiableMap(tmpMap);
            FieldMetaData.addStructMetaDataMap(appendBatch_result.class, metaDataMap);
        }

        public appendBatch_result() {}

        public appendBatch_result(Status success) {
            this();
            this.success = success;
        }

        /**
         * Performs a deep copy on <i>other</i>.
         */
        public appendBatch_result(appendBatch_result other) {
            if (other.isSetSuccess()) {
                this.success = other.success;
            }
        }

        @Override
        public appendBatch_result deepCopy() {
            return new appendBatch_result(this);
        }

        @Override
        public void clear() {
            this.success = null;
        }

        /**
         *
         * @see Status
         */
        @Nullable
        public Status getSuccess() {
            return this.success;
        }

        /**
         *
         * @see Status
         */
        public appendBatch_result setSuccess(@Nullable Status success) {
            this.success = success;
            return this;
        }

        public void unsetSuccess() {
            this.success = null;
        }

        /** Returns true if field success is set (has been assigned a value) and false otherwise */
        public boolean isSetSuccess() {
            return this.success != null;
        }

        public void setSuccessIsSet(boolean value) {
            if (!value) {
                this.success = null;
            }
        }

        @Override
        public void setFieldValue(_Fields field, @Nullable java.lang.Object value) {
            switch (field) {
                case SUCCESS:
                    if (value == null) {
                        unsetSuccess();
                    } else {
                        setSuccess((Status) value);
                    }
                    break;
            }
        }

        @Nullable
        @Override
        public java.lang.Object getFieldValue(_Fields field) {
            switch (field) {
                case SUCCESS:
                    return getSuccess();
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
                case SUCCESS:
                    return isSetSuccess();
            }
            throw new java.lang.IllegalStateException();
        }

        @Override
        public boolean equals(java.lang.Object that) {
            if (that instanceof appendBatch_result) return this.equals((appendBatch_result) that);
            return false;
        }

        public boolean equals(appendBatch_result that) {
            if (that == null) return false;
            if (this == that) return true;

            boolean this_present_success = true && this.isSetSuccess();
            boolean that_present_success = true && that.isSetSuccess();
            if (this_present_success || that_present_success) {
                if (!(this_present_success && that_present_success)) return false;
                if (!this.success.equals(that.success)) return false;
            }

            return true;
        }

        @Override
        public int hashCode() {
            int hashCode = 1;

            hashCode = hashCode * 8191 + ((isSetSuccess()) ? 131071 : 524287);
            if (isSetSuccess()) hashCode = hashCode * 8191 + success.getValue();

            return hashCode;
        }

        @Override
        public int compareTo(appendBatch_result other) {
            if (!getClass().equals(other.getClass())) {
                return getClass().getName().compareTo(other.getClass().getName());
            }

            int lastComparison = 0;

            lastComparison = java.lang.Boolean.compare(isSetSuccess(), other.isSetSuccess());
            if (lastComparison != 0) {
                return lastComparison;
            }
            if (isSetSuccess()) {
                lastComparison = TBaseHelper.compareTo(this.success, other.success);
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

        public void write(TProtocol oprot) throws TException {
            scheme(oprot).write(oprot, this);
        }

        @Override
        public java.lang.String toString() {
            java.lang.StringBuilder sb = new java.lang.StringBuilder("appendBatch_result(");
            boolean first = true;

            sb.append("success:");
            if (this.success == null) {
                sb.append("null");
            } else {
                sb.append(this.success);
            }
            first = false;
            sb.append(")");
            return sb.toString();
        }

        public void validate() throws TException {
            // check for required fields
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

        private static class appendBatch_resultStandardSchemeFactory implements SchemeFactory {
            @Override
            public appendBatch_resultStandardScheme getScheme() {
                return new appendBatch_resultStandardScheme();
            }
        }

        private static class appendBatch_resultStandardScheme extends StandardScheme<appendBatch_result> {

            @Override
            public void read(TProtocol iprot, appendBatch_result struct) throws TException {
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
                            case 0: // SUCCESS
                                if (schemeField.type == TType.I32) {
                                    struct.success = Status.findByValue(iprot.readI32());
                                    struct.setSuccessIsSet(true);
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
            public void write(TProtocol oprot, appendBatch_result struct) throws TException {
                struct.validate();

                oprot.writeStructBegin(STRUCT_DESC);
                if (struct.success != null) {
                    oprot.writeFieldBegin(SUCCESS_FIELD_DESC);
                    oprot.writeI32(struct.success.getValue());
                    oprot.writeFieldEnd();
                }
                oprot.writeFieldStop();
                oprot.writeStructEnd();
            }
        }

        private static class appendBatch_resultTupleSchemeFactory implements SchemeFactory {
            @Override
            public appendBatch_resultTupleScheme getScheme() {
                return new appendBatch_resultTupleScheme();
            }
        }

        private static class appendBatch_resultTupleScheme extends TupleScheme<appendBatch_result> {

            @Override
            public void write(TProtocol prot, appendBatch_result struct) throws TException {
                TTupleProtocol oprot = (TTupleProtocol) prot;
                BitSet optionals = new BitSet();
                if (struct.isSetSuccess()) {
                    optionals.set(0);
                }
                oprot.writeBitSet(optionals, 1);
                if (struct.isSetSuccess()) {
                    oprot.writeI32(struct.success.getValue());
                }
            }

            @Override
            public void read(TProtocol prot, appendBatch_result struct) throws TException {
                prot.incrementRecursionDepth();
                try {
                    TTupleProtocol iprot = (TTupleProtocol) prot;
                    BitSet incoming = iprot.readBitSet(1);
                    if (incoming.get(0)) {
                        struct.success = Status.findByValue(iprot.readI32());
                        struct.setSuccessIsSet(true);
                    }
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
}
