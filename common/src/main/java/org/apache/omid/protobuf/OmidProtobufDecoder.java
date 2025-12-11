/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.omid.protobuf;

import org.apache.phoenix.thirdparty.com.google.protobuf.ExtensionRegistry;
import org.apache.phoenix.thirdparty.com.google.protobuf.ExtensionRegistryLite;
import org.apache.phoenix.thirdparty.com.google.protobuf.Message;
import org.apache.phoenix.thirdparty.com.google.protobuf.MessageLite;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.util.internal.ObjectUtil;

import java.util.List;

/**
 * This class is from netty-codec, but the imports for protobuf are changed to use phoenix-thirdparty
 * so netty would not use other protobuf that might be present on the classpath.
 *
 * Decodes a received {@link ByteBuf} into a
 * <a href="https://github.com/google/protobuf">Google Protocol Buffers</a>
 * {@link Message} and {@link MessageLite}. Please note that this decoder must
 * be used with a proper {@link ByteToMessageDecoder} such as {@link OmidProtobufVarint32FrameDecoder}
 * or {@link LengthFieldBasedFrameDecoder} if you are using a stream-based
 * transport such as TCP/IP. A typical setup for TCP/IP would be:
 * <pre>
 * {@link ChannelPipeline} pipeline = ...;
 *
 * // Decoders
 * pipeline.addLast("frameDecoder",
 *                  new {@link LengthFieldBasedFrameDecoder}(1048576, 0, 4, 0, 4));
 * pipeline.addLast("protobufDecoder",
 *                  new {@link OmidProtobufDecoder}(MyMessage.getDefaultInstance()));
 *
 * // Encoder
 * pipeline.addLast("frameEncoder", new {@link LengthFieldPrepender}(4));
 * pipeline.addLast("protobufEncoder", new {@link OmidProtobufEncoder}());
 * </pre>
 * and then you can use a {@code MyMessage} instead of a {@link ByteBuf}
 * as a message:
 * <pre>
 * void channelRead({@link ChannelHandlerContext} ctx, Object msg) {
 *     MyMessage req = (MyMessage) msg;
 *     MyMessage res = MyMessage.newBuilder().setText(
 *                               "Did you say '" + req.getText() + "'?").build();
 *     ch.write(res);
 * }
 * </pre>
 */
@Sharable
public class OmidProtobufDecoder extends MessageToMessageDecoder<ByteBuf> {

    private static final boolean HAS_PARSER;

    static {
        boolean hasParser = false;
        try {
            // MessageLite.getParserForType() is not available until protobuf 2.5.0.
            MessageLite.class.getDeclaredMethod("getParserForType");
            hasParser = true;
        } catch (Throwable t) {
            // Ignore
        }

        HAS_PARSER = hasParser;
    }

    private final MessageLite prototype;
    private final ExtensionRegistryLite extensionRegistry;

    /**
     * Creates a new instance.
     */
    public OmidProtobufDecoder(MessageLite prototype) {
        this(prototype, null);
    }

    public OmidProtobufDecoder(MessageLite prototype, ExtensionRegistry extensionRegistry) {
        this(prototype, (ExtensionRegistryLite) extensionRegistry);
    }

    public OmidProtobufDecoder(MessageLite prototype, ExtensionRegistryLite extensionRegistry) {
        this.prototype = ObjectUtil.checkNotNull(prototype, "prototype").getDefaultInstanceForType();
        this.extensionRegistry = extensionRegistry;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out)
            throws Exception {
        final byte[] array;
        final int offset;
        final int length = msg.readableBytes();
        if (msg.hasArray()) {
            array = msg.array();
            offset = msg.arrayOffset() + msg.readerIndex();
        } else {
            array = ByteBufUtil.getBytes(msg, msg.readerIndex(), length, false);
            offset = 0;
        }

        if (extensionRegistry == null) {
            if (HAS_PARSER) {
                out.add(prototype.getParserForType().parseFrom(array, offset, length));
            } else {
                out.add(prototype.newBuilderForType().mergeFrom(array, offset, length).build());
            }
        } else {
            if (HAS_PARSER) {
                out.add(prototype.getParserForType().parseFrom(
                        array, offset, length, extensionRegistry));
            } else {
                out.add(prototype.newBuilderForType().mergeFrom(
                        array, offset, length, extensionRegistry).build());
            }
        }
    }
}
