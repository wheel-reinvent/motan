/*
 *  Copyright 2009-2016 Weibo, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.weibo.api.motan.protocol.rpc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Map;

import com.weibo.api.motan.codec.AbstractCodec;
import com.weibo.api.motan.codec.Serialization;
import com.weibo.api.motan.common.MotanConstants;
import com.weibo.api.motan.common.URLParamType;
import com.weibo.api.motan.core.extension.ExtensionLoader;
import com.weibo.api.motan.core.extension.SpiMeta;
import com.weibo.api.motan.exception.MotanErrorMsgConstant;
import com.weibo.api.motan.exception.MotanFrameworkException;
import com.weibo.api.motan.rpc.DefaultRequest;
import com.weibo.api.motan.rpc.DefaultResponse;
import com.weibo.api.motan.rpc.Request;
import com.weibo.api.motan.rpc.Response;
import com.weibo.api.motan.transport.Channel;
import com.weibo.api.motan.util.ByteUtil;
import com.weibo.api.motan.util.ExceptionUtil;
import com.weibo.api.motan.util.ReflectUtil;

/**
 * @author maijunsheng
 * @version 创建时间：2013-5-22
 *
 */
@SpiMeta(name = "motan")
public class DefaultRpcCodec extends AbstractCodec {
    private static final short MAGIC = (short) 0xF0F0;

    private static final byte MASK = 0x07;

    /**
     * 编码
     */
    @Override
    public byte[] encode(Channel channel, Object message) throws IOException {
        // 根据message的类型进行对应的编码，如果类型不支持则抛出框架异常
        try {
            if (message instanceof Request) {
                return encodeRequest(channel, (Request) message);
            } else if (message instanceof Response) {
                return encodeResponse(channel, (Response) message);
            }
        } catch (Exception e) {
            if (ExceptionUtil.isMotanException(e)) {
                // 运行时异常
                throw (RuntimeException) e;
            } else {
                throw new MotanFrameworkException("encode error: isResponse=" + (message instanceof Response), e,
                        MotanErrorMsgConstant.FRAMEWORK_ENCODE_ERROR);
            }
        }

        throw new MotanFrameworkException("encode error: message type not support, " + message.getClass(),
                MotanErrorMsgConstant.FRAMEWORK_ENCODE_ERROR);
    }

    /**
     * decode data
     *
     * <pre>
	 * 		对于client端：主要是来自server端的response or exception
	 * 		对于server端: 主要是来自client端的request
	 * </pre>
     *
     * @param data
     * @return
     * @throws IOException
     */
    @Override
    public Object decode(Channel channel, String remoteIp, byte[] data) throws IOException {
        // 如果数据的长度小于或者等于V1版本的头部长度，抛出异常
        if (data.length <= RpcProtocolVersion.VERSION_1.getHeaderLength()) {
            throw new MotanFrameworkException("decode error: format problem", MotanErrorMsgConstant.FRAMEWORK_DECODE_ERROR);
        }

        // 判断MAGIC 0-15 bit
        short type = ByteUtil.bytes2short(data, 0);

        if (type != MAGIC) {
            throw new MotanFrameworkException("decode error: magic error", MotanErrorMsgConstant.FRAMEWORK_DECODE_ERROR);
        }

        // 判断版本 16-23 bit
        if (data[2] != RpcProtocolVersion.VERSION_1.getVersion()) {
            throw new MotanFrameworkException("decode error: version error", MotanErrorMsgConstant.FRAMEWORK_DECODE_ERROR);
        }

        // body长度 96-127bit
        int bodyLength = ByteUtil.bytes2int(data, 12);
        // 判断长度 header+body
        if (RpcProtocolVersion.VERSION_1.getHeaderLength() + bodyLength != data.length) {
            throw new MotanFrameworkException("decode error: content length error", MotanErrorMsgConstant.FRAMEWORK_DECODE_ERROR);
        }

        // 根据flag和MASK确定消息体的类型(Request/Response和数据的类型(对于响应))
        byte flag = data[3];
        byte dataType = (byte) (flag & MASK);
        boolean isResponse = (dataType != MotanConstants.FLAG_REQUEST);

        // 获取body的数据
        byte[] body = new byte[bodyLength];

        System.arraycopy(data, RpcProtocolVersion.VERSION_1.getHeaderLength(), body, 0, bodyLength);

        // 获取requestId
        long requestId = ByteUtil.bytes2long(data, 4);
        // 获取序列化实例
        Serialization serialization =
                ExtensionLoader.getExtensionLoader(Serialization.class).getExtension(
                        channel.getUrl().getParameter(URLParamType.serialize.getName(), URLParamType.serialize.getValue()));

        try {
            // 解码
            if (isResponse) { // response
                return decodeResponse(body, dataType, requestId, serialization);
            } else {
                return decodeRequest(body, requestId, serialization);
            }
        } catch (ClassNotFoundException e) {
            throw new MotanFrameworkException("decode " + (isResponse ? "response" : "request") + " error: class not found", e,
                    MotanErrorMsgConstant.FRAMEWORK_DECODE_ERROR);
        } catch (Exception e) {
            if (ExceptionUtil.isMotanException(e)) {
                throw (RuntimeException) e;
            } else {
                throw new MotanFrameworkException("decode error: isResponse=" + isResponse, e, MotanErrorMsgConstant.FRAMEWORK_DECODE_ERROR);
            }
        }
    }

    /**
     * request body 数据：
     *
     * <pre>
	 *
	 * 	 body:
	 *
	 * 	 byte[] data :
	 *
	 * 	 序列化接口名/方法名/方法参数描述/方法值/附加大小/附加值
     * 	 serialize(interface_name, method_name, method_param_desc, method_param_value, attachments_size, attachments_value)
	 *
	 *   method_param_desc:  for_each (string.append(method_param_interface_name))
	 *
	 *   method_param_value: for_each (method_param_name, method_param_value)
	 *
	 * 	 attachments_value:  for_each (attachment_name, attachment_value)
	 *
	 * </pre>
     *
     * @param request
     * @return
     * @throws IOException
     */
    private byte[] encodeRequest(Channel channel, Request request) throws IOException {
        // 通过对象输出流创建对象输出，并写入请求的接口名称/方法名称/参数描述
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ObjectOutput output = createOutput(outputStream);
        output.writeUTF(request.getInterfaceName());
        output.writeUTF(request.getMethodName());
        output.writeUTF(request.getParamtersDesc());

        // 获取序列化实例
        Serialization serialization =
                ExtensionLoader.getExtensionLoader(Serialization.class).getExtension(
                        channel.getUrl().getParameter(URLParamType.serialize.getName(), URLParamType.serialize.getValue()));

        //遍历参数并序列化写入对象输出中
        if (request.getArguments() != null && request.getArguments().length > 0) {
            for (Object obj : request.getArguments()) {
                serialize(output, obj, serialization);
            }
        }

        // 写入附加大小，如果附加不为空遍历所有的附加写入每个附加的key和value
        if (request.getAttachments() == null || request.getAttachments().isEmpty()) {
            // empty attachments
            output.writeInt(0);
        } else {
            output.writeInt(request.getAttachments().size());
            for (Map.Entry<String, String> entry : request.getAttachments().entrySet()) {
                output.writeUTF(entry.getKey());
                output.writeUTF(entry.getValue());
            }
        }

        // 清空缓冲区获取数据
        output.flush();
        byte[] body = outputStream.toByteArray();

        byte flag = MotanConstants.FLAG_REQUEST;
        //关闭输出流
        output.close();

        return encode(body, flag, request.getRequestId());
    }

    /**
     * response body 数据：
     *
     * <pre>
	 *
	 * body:
	 *
	 * 	 byte[] :  serialize (result) or serialize (exception)
	 *
	 * </pre>
     *
     * @param channel
     * @param value
     * @return
     * @throws IOException
     */
    private byte[] encodeResponse(Channel channel, Response value) throws IOException {
        // 通过对象输出流创建对象输出
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ObjectOutput output = createOutput(outputStream);
        // 获取序列化实例
        Serialization serialization =
                ExtensionLoader.getExtensionLoader(Serialization.class).getExtension(
                        channel.getUrl().getParameter(URLParamType.serialize.getName(), URLParamType.serialize.getValue()));

        byte flag = 0;

        // 写入处理时间
        output.writeLong(value.getProcessTime());

        if (value.getException() != null) {
            // 如果是异常写入异常名和异常并设置flag
            output.writeUTF(value.getException().getClass().getName());
            serialize(output, value.getException(), serialization);
            flag = MotanConstants.FLAG_RESPONSE_EXCEPTION;
        } else if (value.getValue() == null) {
            // 如果是空响应 设置flag
            flag = MotanConstants.FLAG_RESPONSE_VOID;
        } else {
            // 正常响应 写入响应的bean的名字和响应 并设置flag
            output.writeUTF(value.getValue().getClass().getName());
            serialize(output, value.getValue(), serialization);
            flag = MotanConstants.FLAG_RESPONSE;
        }

        // 清空缓冲区获取数据
        output.flush();

        byte[] body = outputStream.toByteArray();

        // 关闭输出流
        output.close();

        return encode(body, flag, value.getRequestId());
    }

    /**
     * 数据协议：
     *
     * <pre>
	 *
	 * header:  16个字节
	 *
	 * 0-15 bit 	:  magic
	 * 16-23 bit	:  version
	 * 24-31 bit	:  extend flag , 其中： 29-30 bit: event 可支持4种event，比如normal, exception等,  31 bit : 0 is request , 1 is response
	 * 32-95 bit 	:  request id
	 * 96-127 bit 	:  body content length
	 *
	 * </pre>
     *
     * @param body
     * @param flag
     * @param requestId
     * @return
     * @throws IOException
     */
    private byte[] encode(byte[] body, byte flag, long requestId) throws IOException {
        byte[] header = new byte[RpcProtocolVersion.VERSION_1.getHeaderLength()];
        int offset = 0;

        // 0 - 15 bit : magic 魔数
        ByteUtil.short2bytes(MAGIC, header, offset);
        offset += 2;

        // 16 - 23 bit : version 版本
        header[offset++] = RpcProtocolVersion.VERSION_1.getVersion();

        // 24 - 31 bit : extend flag 扩展标记
        header[offset++] = flag;

        // 32 - 95 bit : requestId
        ByteUtil.long2bytes(requestId, header, offset);
        offset += 8;

        // 96 - 127 bit : body content length 内容体长度
        ByteUtil.int2bytes(body.length, header, offset);

        byte[] data = new byte[header.length + body.length];

        System.arraycopy(header, 0, data, 0, header.length);
        System.arraycopy(body, 0, data, header.length, body.length);

        return data;
    }

    private Object decodeRequest(byte[] body, long requestId, Serialization serialization) throws IOException, ClassNotFoundException {
        // 通过对象输入流创建对象输入
        ByteArrayInputStream inputStream = new ByteArrayInputStream(body);
        ObjectInput input = createInput(inputStream);

        // 读取接口名称、方法名称、参数描述
        String interfaceName = input.readUTF();
        String methodName = input.readUTF();
        String paramtersDesc = input.readUTF();

        DefaultRequest rpcRequest = new DefaultRequest();
        rpcRequest.setRequestId(requestId);
        rpcRequest.setInterfaceName(interfaceName);
        rpcRequest.setMethodName(methodName);
        rpcRequest.setParamtersDesc(paramtersDesc);
        rpcRequest.setArguments(decodeRequestParameter(input, paramtersDesc, serialization));
        rpcRequest.setAttachments(decodeRequestAttachments(input));

        input.close();

        return rpcRequest;
    }

    // 解码请求参数
    private Object[] decodeRequestParameter(ObjectInput input, String parameterDesc, Serialization serialization) throws IOException,
            ClassNotFoundException {
        if (parameterDesc == null || parameterDesc.equals("")) {
            return null;
        }

        // 取得参数描述对应的类数组
        Class<?>[] classTypes = ReflectUtil.forNames(parameterDesc);

        // 初始化对象数组
        Object[] paramObjs = new Object[classTypes.length];

        // 遍历类数组将数据反序列化为对象实例
        for (int i = 0; i < classTypes.length; i++) {
            paramObjs[i] = deserialize((byte[]) input.readObject(), classTypes[i], serialization);
        }

        return paramObjs;
    }

    // 解码请求附加
    private Map<String, String> decodeRequestAttachments(ObjectInput input) throws IOException, ClassNotFoundException {
        // 判断附加的大小
        int size = input.readInt();

        if (size <= 0) {
            return null;
        }

        Map<String, String> attachments = new HashMap<String, String>();

        // 将每个附加设置到对应的map中
        for (int i = 0; i < size; i++) {
            attachments.put(input.readUTF(), input.readUTF());
        }

        return attachments;
    }

    private Object decodeResponse(byte[] body, byte dataType, long requestId, Serialization serialization) throws IOException,
            ClassNotFoundException {

        // 通过对象输入流创建对象输入
        ByteArrayInputStream inputStream = new ByteArrayInputStream(body);
        ObjectInput input = createInput(inputStream);

        // 处理时间
        long processTime = input.readLong();

        // 创建默认的响应实例 设置requestId和处理时间
        DefaultResponse response = new DefaultResponse();
        response.setRequestId(requestId);
        response.setProcessTime(processTime);

        // 如果响应类型是VOID直接返回
        if (dataType == MotanConstants.FLAG_RESPONSE_VOID) {
            return response;
        }

        // 读取类名，根据类名取得对应的类
        String className = input.readUTF();
        Class<?> clz = ReflectUtil.forName(className);

        // 反序列化为实例
        Object result = deserialize((byte[]) input.readObject(), clz, serialization);

        // 将反序列化的实例设置到响应的值或者异常中
        if (dataType == MotanConstants.FLAG_RESPONSE) {
            response.setValue(result);
        } else if (dataType == MotanConstants.FLAG_RESPONSE_EXCEPTION) {
            response.setException((Exception) result);
        } else {
            throw new MotanFrameworkException("decode error: response dataType not support " + dataType,
                    MotanErrorMsgConstant.FRAMEWORK_DECODE_ERROR);
        }

        response.setRequestId(requestId);

        // 关闭输入流
        input.close();

        return response;
    }

}
