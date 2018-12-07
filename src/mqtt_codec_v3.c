// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

#include <stdlib.h>
#include <limits.h>
#include "azure_c_shared_utility/optimize_size.h"
#include "azure_c_shared_utility/gballoc.h"
#include "azure_c_shared_utility/buffer_.h"
#include "azure_c_shared_utility/strings.h"
#include "azure_c_shared_utility/macro_utils.h"
#include "azure_c_shared_utility/xlogging.h"

#include "azure_umqtt_c/mqtt_codec_v3.h"
#include "azure_umqtt_c/mqtt_codec_info.h"
#include "azure_umqtt_c/mqtt_codec_util.h"

#include <inttypes.h>

#define PAYLOAD_OFFSET                      5

#define USERNAME_FLAG                       0x80
#define PASSWORD_FLAG                       0x40
#define WILL_RETAIN_FLAG                    0x20
#define WILL_QOS_FLAG_                      0x18
#define WILL_FLAG_FLAG                      0x04
#define CLEAN_SESSION_FLAG                  0x02

#define NEXT_128_CHUNK                      0x80
#define PUBLISH_DUP_FLAG                    0x8
#define PUBLISH_QOS_EXACTLY_ONCE            0x4
#define PUBLISH_QOS_AT_LEAST_ONCE           0x2
#define PUBLISH_QOS_RETAIN                  0x1

#define PROTOCOL_NUMBER                     4
#define CONN_FLAG_BYTE_OFFSET               7

#define CONNECT_FIXED_HEADER_SIZE           2
#define CONNECT_VARIABLE_HEADER_SIZE        10
#define SUBSCRIBE_FIXED_HEADER_FLAG         0x2
#define UNSUBSCRIBE_FIXED_HEADER_FLAG       0x2

#define MAX_SEND_SIZE                       0xFFFFFF7F

typedef struct CODEC_V3_INSTANCE_TAG
{
    CONTROL_PACKET_TYPE currPacket;
    CODEC_STATE_RESULT codecState;
    size_t bufferOffset;
    int headerFlags;
    BUFFER_HANDLE headerData;
    ON_PACKET_COMPLETE_CALLBACK packetComplete;
    void* callContext;
    uint8_t storeRemainLen[4];
    size_t remainLenIndex;

    TRACE_LOG_VALUE trace_func;
    void* trace_ctx;
} CODEC_V3_INSTANCE;

typedef struct PUBLISH_HEADER_INFO_TAG
{
    const char* topicName;
    uint16_t packetId;
    const char* msgBuffer;
    QOS_VALUE qualityOfServiceValue;
} PUBLISH_HEADER_INFO;

static int addListItemsToUnsubscribePacket(CODEC_V3_INSTANCE* mqtt_codec, BUFFER_HANDLE ctrlPacket, const char** payloadList, size_t payloadCount)
{
    int result = 0;
    size_t index = 0;
    for (index = 0; index < payloadCount && result == 0; index++)
    {
        // Add the Payload
        size_t offsetLen = BUFFER_length(ctrlPacket);
        size_t topicLen = strlen(payloadList[index]);
        if (topicLen > USHRT_MAX)
        {
            LogError("Failure Topic length is greater than max size");
            result = __FAILURE__;
        }
        else if (BUFFER_enlarge(ctrlPacket, topicLen + 2) != 0)
        {
            LogError("Failure enlarging buffer");
            result = __FAILURE__;
        }
        else
        {
            uint8_t* iterator = BUFFER_u_char(ctrlPacket);
            iterator += offsetLen;
            byteutil_writeUTF(&iterator, payloadList[index], (uint16_t)topicLen);
            if (mqtt_codec->trace_func != NULL)
            {
                mqtt_codec->trace_func(mqtt_codec->trace_ctx, " | TOPIC_NAME: %s", payloadList[index]);
            }
        }
    }
    return result;
}

static int addListItemsToSubscribePacket(CODEC_V3_INSTANCE* mqtt_codec, BUFFER_HANDLE ctrlPacket, SUBSCRIBE_PAYLOAD* payloadList, size_t payloadCount)
{
    int result = 0;
    size_t index = 0;
    for (index = 0; index < payloadCount && result == 0; index++)
    {
        // Add the Payload
        size_t offsetLen = BUFFER_length(ctrlPacket);
        size_t topicLen = strlen(payloadList[index].subscribeTopic);
        if (topicLen > USHRT_MAX)
        {
            LogError("Failure Topic length is greater than max size");
            result = __FAILURE__;
        }
        else if (BUFFER_enlarge(ctrlPacket, topicLen + 2 + 1) != 0)
        {
            LogError("Failure enlarging buffer");
            result = __FAILURE__;
        }
        else
        {
            uint8_t* iterator = BUFFER_u_char(ctrlPacket);
            iterator += offsetLen;
            byteutil_writeUTF(&iterator, payloadList[index].subscribeTopic, (uint16_t)topicLen);
            *iterator = payloadList[index].qosReturn;

            if (mqtt_codec->trace_func != NULL)
            {
                mqtt_codec->trace_func(mqtt_codec->trace_ctx, " | TOPIC_NAME: %s | QOS: %d", payloadList[index].subscribeTopic, (int)payloadList[index].qosReturn);
            }
        }
    }
    return result;
}

static int constructConnectVariableHeader(CODEC_V3_INSTANCE* mqtt_codec, BUFFER_HANDLE ctrlPacket, const MQTT_CLIENT_OPTIONS* mqttOptions)
{
    int result = 0;
    if (BUFFER_enlarge(ctrlPacket, CONNECT_VARIABLE_HEADER_SIZE) != 0)
    {
        LogError("Failure enlarging buffer");
        result = __FAILURE__;
    }
    else
    {
        uint8_t* iterator = BUFFER_u_char(ctrlPacket);
        if (iterator == NULL)
        {
            result = __FAILURE__;
        }
        else
        {
            if (mqtt_codec->trace_func != NULL)
            {
                mqtt_codec->trace_func(mqtt_codec->trace_ctx, " | VER: %d | KEEPALIVE: %d", PROTOCOL_NUMBER, mqttOptions->keepAliveInterval);
            }
            byteutil_writeUTF(&iterator, "MQTT", 4);
            byteutil_writeByte(&iterator, PROTOCOL_NUMBER);
            byteutil_writeByte(&iterator, 0); // Flags will be entered later
            byteutil_writeInt(&iterator, mqttOptions->keepAliveInterval);
            result = 0;
        }
    }
    return result;
}

static int constructPublishVariableHeader(CODEC_V3_INSTANCE* mqtt_codec, BUFFER_HANDLE ctrlPacket, const PUBLISH_HEADER_INFO* publishHeader)
{
    int result = 0;
    size_t topicLen = 0;
    size_t spaceLen = 0;
    size_t idLen = 0;

    size_t currLen = BUFFER_length(ctrlPacket);

    topicLen = strlen(publishHeader->topicName);
    spaceLen += 2;

    if (publishHeader->qualityOfServiceValue != DELIVER_AT_MOST_ONCE)
    {
        // Packet Id is only set if the QOS is not 0
        idLen = 2;
    }

    if (topicLen > USHRT_MAX)
    {
        result = __FAILURE__;
    }
    else if (BUFFER_enlarge(ctrlPacket, topicLen + idLen + spaceLen) != 0)
    {
        result = __FAILURE__;
    }
    else
    {
        uint8_t* iterator = BUFFER_u_char(ctrlPacket);
        if (iterator == NULL)
        {
            result = __FAILURE__;
        }
        else
        {
            iterator += currLen;
            /* The Topic Name MUST be present as the first field in the PUBLISH Packet Variable header.It MUST be 792 a UTF-8 encoded string [MQTT-3.3.2-1] as defined in section 1.5.3.*/
            byteutil_writeUTF(&iterator, publishHeader->topicName, (uint16_t)topicLen);
            if (mqtt_codec->trace_func != NULL)
            {
                mqtt_codec->trace_func(mqtt_codec->trace_ctx, " | TOPIC_NAME: %s", publishHeader->topicName);
            }
            if (idLen > 0)
            {
                if (mqtt_codec->trace_func != NULL)
                {
                    mqtt_codec->trace_func(mqtt_codec->trace_ctx, " | PACKET_ID: %"PRIu16, publishHeader->packetId);
                }
                byteutil_writeInt(&iterator, publishHeader->packetId);
            }
            result = 0;
        }
    }
    return result;
}

static int constructSubscibeTypeVariableHeader(BUFFER_HANDLE ctrlPacket, uint16_t packetId)
{
    int result = 0;
    if (BUFFER_enlarge(ctrlPacket, 2) != 0)
    {
        result = __FAILURE__;
    }
    else
    {
        uint8_t* iterator = BUFFER_u_char(ctrlPacket);
        if (iterator == NULL)
        {
            result = __FAILURE__;
        }
        else
        {
            byteutil_writeInt(&iterator, packetId);
            result = 0;
        }
    }
    return result;
}

static BUFFER_HANDLE constructPublishReply(CONTROL_PACKET_TYPE type, uint8_t flags, uint16_t packetId)
{
    BUFFER_HANDLE result = BUFFER_create_with_size(4);
    if (result == NULL)
    {
        LogError(FAILURE_MSG_CREATE_BUFFER);
    }
    else
    {
        uint8_t* iterator = BUFFER_u_char(result);
        *iterator = (uint8_t)type | flags;
        iterator++;
        *iterator = 0x2;
        iterator++;
        byteutil_writeInt(&iterator, packetId);
    }
    return result;
}

static int constructFixedHeader(BUFFER_HANDLE ctrlPacket, CONTROL_PACKET_TYPE packetType, uint8_t flags)
{
    int result;
    if (ctrlPacket == NULL)
    {
        return __FAILURE__;
    }
    else
    {
        size_t packetLen = BUFFER_length(ctrlPacket);
        uint8_t remainSize[4] ={ 0 };
        size_t index = 0;

        // Calculate the length of packet
        do
        {
            uint8_t encode = packetLen % 128;
            packetLen /= 128;
            // if there are more data to encode, set the top bit of this byte
            if (packetLen > 0)
            {
                encode |= NEXT_128_CHUNK;
            }
            remainSize[index++] = encode;
        } while (packetLen > 0);

        BUFFER_HANDLE fixedHeader = BUFFER_new();
        if (fixedHeader == NULL)
        {
            result = __FAILURE__;
        }
        else if (BUFFER_pre_build(fixedHeader, index + 1) != 0)
        {
            BUFFER_delete(fixedHeader);
            result = __FAILURE__;
        }
        else
        {
            uint8_t* iterator = BUFFER_u_char(fixedHeader);
            *iterator = (uint8_t)packetType | flags;
            iterator++;
            (void)memcpy(iterator, remainSize, index);

            result = BUFFER_prepend(ctrlPacket, fixedHeader);
            BUFFER_delete(fixedHeader);
        }
    }
    return result;
}

static int constructConnPayload(CODEC_V3_INSTANCE* mqtt_codec, BUFFER_HANDLE ctrlPacket, const MQTT_CLIENT_OPTIONS* mqttOptions)
{
    int result = 0;
    if (mqttOptions == NULL || ctrlPacket == NULL)
    {
        result = __FAILURE__;
    }
    else
    {
        size_t clientLen = 0;
        size_t usernameLen = 0;
        size_t passwordLen = 0;
        size_t willMessageLen = 0;
        size_t willTopicLen = 0;
        size_t spaceLen = 0;

        if (mqttOptions->clientId != NULL)
        {
            spaceLen += 2;
            clientLen = strlen(mqttOptions->clientId);
        }
        if (mqttOptions->username != NULL)
        {
            spaceLen += 2;
            usernameLen = strlen(mqttOptions->username);
        }
        if (mqttOptions->password != NULL)
        {
            spaceLen += 2;
            passwordLen = strlen(mqttOptions->password);
        }
        if (mqttOptions->willMessage != NULL)
        {
            spaceLen += 2;
            willMessageLen = strlen(mqttOptions->willMessage);
        }
        if (mqttOptions->willTopic != NULL)
        {
            spaceLen += 2;
            willTopicLen = strlen(mqttOptions->willTopic);
        }

        size_t currLen = BUFFER_length(ctrlPacket);
        size_t totalLen = clientLen + usernameLen + passwordLen + willMessageLen + willTopicLen + spaceLen;

        // Validate the Username & Password
        if (clientLen > USHRT_MAX)
        {
            result = __FAILURE__;
        }
        else if (usernameLen == 0 && passwordLen > 0)
        {
            result = __FAILURE__;
        }
        else if ((willMessageLen > 0 && willTopicLen == 0) || (willTopicLen > 0 && willMessageLen == 0))
        {
            result = __FAILURE__;
        }
        else if (BUFFER_enlarge(ctrlPacket, totalLen) != 0)
        {
            result = __FAILURE__;
        }
        else
        {
            uint8_t* packet = BUFFER_u_char(ctrlPacket);
            uint8_t* iterator = packet;

            iterator += currLen;
            byteutil_writeUTF(&iterator, mqttOptions->clientId, (uint16_t)clientLen);

            // TODO: Read on the Will Topic
            if (willMessageLen > USHRT_MAX || willTopicLen > USHRT_MAX || usernameLen > USHRT_MAX || passwordLen > USHRT_MAX)
            {
                result = __FAILURE__;
            }
            else
            {
                if (willMessageLen > 0 && willTopicLen > 0)
                {
                    if (mqtt_codec->trace_func != NULL)
                    {
                        mqtt_codec->trace_func(mqtt_codec->trace_ctx, " | WILL_TOPIC: %s", mqttOptions->willTopic);
                    }
                    packet[CONN_FLAG_BYTE_OFFSET] |= WILL_FLAG_FLAG;
                    byteutil_writeUTF(&iterator, mqttOptions->willTopic, (uint16_t)willTopicLen);
                    packet[CONN_FLAG_BYTE_OFFSET] |= (mqttOptions->qualityOfServiceValue << 3);
                    if (mqttOptions->messageRetain)
                    {
                        packet[CONN_FLAG_BYTE_OFFSET] |= WILL_RETAIN_FLAG;
                    }
                    byteutil_writeUTF(&iterator, mqttOptions->willMessage, (uint16_t)willMessageLen);
                }
                if (usernameLen > 0)
                {
                    packet[CONN_FLAG_BYTE_OFFSET] |= USERNAME_FLAG;
                    byteutil_writeUTF(&iterator, mqttOptions->username, (uint16_t)usernameLen);
                    if (mqtt_codec->trace_func != NULL)
                    {
                        mqtt_codec->trace_func(mqtt_codec->trace_ctx, " | USERNAME: %s", mqttOptions->username);
                    }
                }
                if (passwordLen > 0)
                {
                    packet[CONN_FLAG_BYTE_OFFSET] |= PASSWORD_FLAG;
                    byteutil_writeUTF(&iterator, mqttOptions->password, (uint16_t)passwordLen);
                    if (mqtt_codec->trace_func != NULL)
                    {
                        mqtt_codec->trace_func(mqtt_codec->trace_ctx, " | PWD: XXXX");
                    }
                }
                // TODO: Get the rest of the flags
                if (mqtt_codec->trace_func != NULL)
                {
                    mqtt_codec->trace_func(mqtt_codec->trace_ctx, " | CLEAN: %s", mqttOptions->useCleanSession ? "1" : "0");
                }
                if (mqttOptions->useCleanSession)
                {
                    packet[CONN_FLAG_BYTE_OFFSET] |= CLEAN_SESSION_FLAG;
                }
                if (mqtt_codec->trace_func != NULL)
                {
                    mqtt_codec->trace_func(mqtt_codec->trace_ctx, "  | FLAGS: %ul", packet[CONN_FLAG_BYTE_OFFSET]);
                }
                result = 0;
            }
        }
    }
    return result;
}

static int prepareheaderDataInfo(CODEC_V3_INSTANCE* codecData, uint8_t remainLen)
{
    int result;
    if (codecData == NULL)
    {
        result = __FAILURE__;
    }
    else
    {
        result = 0;
        codecData->storeRemainLen[codecData->remainLenIndex++] = remainLen;
        if (remainLen <= 0x7f)
        {
            int multiplier = 1;
            int totalLen = 0;
            size_t index = 0;
            uint8_t encodeByte = 0;
            do
            {
                encodeByte = codecData->storeRemainLen[index++];
                totalLen += (encodeByte & 127) * multiplier;
                multiplier *= NEXT_128_CHUNK;

                if (multiplier > 128 * 128 * 128)
                {
                    result = __FAILURE__;
                    break;
                }
            } while ((encodeByte & NEXT_128_CHUNK) != 0);

            codecData->codecState = CODEC_STATE_VAR_HEADER;

            // Reset remainLen Index
            codecData->remainLenIndex = 0;
            memset(codecData->storeRemainLen, 0, 4 * sizeof(uint8_t));

            if (totalLen > 0)
            {
                codecData->bufferOffset = 0;
                codecData->headerData = BUFFER_new();
                if (codecData->headerData == NULL)
                {
                    /* Codes_SRS_MQTT_CODEC_07_035: [ If any error is encountered then the packet state will be marked as error and mqtt_codec_bytesReceived shall return a non-zero value. ] */
                    LogError("Failed BUFFER_new");
                    result = __FAILURE__;
                }
                else
                {
                    if (BUFFER_pre_build(codecData->headerData, totalLen) != 0)
                    {
                        /* Codes_SRS_MQTT_CODEC_07_035: [ If any error is encountered then the packet state will be marked as error and mqtt_codec_bytesReceived shall return a non-zero value. ] */
                        LogError("Failed BUFFER_pre_build");
                        result = __FAILURE__;
                    }

                }
            }
        }
    }
    return result;
}

static void completePacketData(CODEC_V3_INSTANCE* codecData)
{
    if (codecData)
    {
        if (codecData->packetComplete != NULL)
        {
            codecData->packetComplete(codecData->callContext, codecData->currPacket, codecData->headerFlags, codecData->headerData);
        }

        // Clean up data
        codecData->currPacket = UNKNOWN_TYPE;
        codecData->codecState = CODEC_STATE_FIXED_HEADER;
        codecData->headerFlags = 0;
        BUFFER_delete(codecData->headerData);
        codecData->headerData = NULL;
    }
}

MQTT_CODEC_HANDLE codec_v3_create(ON_PACKET_COMPLETE_CALLBACK packetComplete, void* callbackCtx)
{
    CODEC_V3_INSTANCE* result;
    result = malloc(sizeof(CODEC_V3_INSTANCE));
    /* Codes_SRS_MQTT_CODEC_07_001: [If a failure is encountered then mqtt_codec_create shall return NULL.] */
    if (result != NULL)
    {
        memset(result, 0, sizeof(CODEC_V3_INSTANCE));
        /* Codes_SRS_MQTT_CODEC_07_002: [On success mqtt_codec_create shall return a MQTTCODEC_HANDLE value.] */
        result->currPacket = UNKNOWN_TYPE;
        result->codecState = CODEC_STATE_FIXED_HEADER;
        result->packetComplete = packetComplete;
        result->callContext = callbackCtx;
        memset(result->storeRemainLen, 0, 4 * sizeof(uint8_t));
        result->remainLenIndex = 0;
    }
    return (MQTT_CODEC_HANDLE)result;
}

void codec_v3_destroy(MQTT_CODEC_HANDLE handle)
{
    /* Codes_SRS_MQTT_CODEC_07_003: [If the handle parameter is NULL then mqtt_codec_destroy shall do nothing.] */
    if (handle != NULL)
    {
        CODEC_V3_INSTANCE* codec_data = (CODEC_V3_INSTANCE*)handle;
        /* Codes_SRS_MQTT_CODEC_07_004: [mqtt_codec_destroy shall deallocate all memory that has been allocated by this object.] */
        BUFFER_delete(codec_data->headerData);
        free(codec_data);
    }
}

BUFFER_HANDLE codec_v3_connect(MQTT_CODEC_HANDLE handle, const MQTT_CLIENT_OPTIONS* mqttOptions)
{
    BUFFER_HANDLE result;
    /* Codes_SRS_MQTT_CODEC_07_008: [If the parameters mqttOptions is NULL then mqtt_codec_connect shall return a null value.] */
    if (mqttOptions == NULL || handle == NULL)
    {
        LogError("Invalid argument specified: mqttOptions: %p, handle: %p", mqttOptions, handle);
        result = NULL;
    }
    else
    {
        CODEC_V3_INSTANCE* mqtt_codec = (CODEC_V3_INSTANCE*)handle;
        /* Codes_SRS_MQTT_CODEC_07_009: [mqtt_codec_connect shall construct a BUFFER_HANDLE that represents a MQTT CONNECT packet.] */
        result = BUFFER_new();
        if (result != NULL)
        {
            if (mqtt_codec->trace_func != NULL)
            {
                mqtt_codec->trace_func(mqtt_codec->trace_ctx, "CONNECT");
            }

            // Add Variable Header Information
            if (constructConnectVariableHeader(mqtt_codec, result, mqttOptions) != 0)
            {
                LogError("Failure constructing variable header");
                /* Codes_SRS_MQTT_CODEC_07_010: [If any error is encountered then mqtt_codec_connect shall return NULL.] */
                BUFFER_delete(result);
                result = NULL;
            }
            else if (constructConnPayload(mqtt_codec, result, mqttOptions) != 0)
            {
                /* Codes_SRS_MQTT_CODEC_07_010: [If any error is encountered then mqtt_codec_connect shall return NULL.] */
                LogError("Failure constructing payload");
                BUFFER_delete(result);
                result = NULL;
            }
            else if (constructFixedHeader(result, CONNECT_TYPE, 0) != 0)
            {
                /* Codes_SRS_MQTT_CODEC_07_010: [If any error is encountered then mqtt_codec_connect shall return NULL.] */
                LogError("Failure constructing fixed");
                BUFFER_delete(result);
                result = NULL;
            }
        }
    }
    return result;
}

BUFFER_HANDLE codec_v3_disconnect(MQTT_CODEC_HANDLE handle)
{
    (void)handle;
    /* Codes_SRS_MQTT_CODEC_07_011: [On success mqtt_codec_disconnect shall construct a BUFFER_HANDLE that represents a MQTT DISCONNECT packet.] */
    BUFFER_HANDLE result = BUFFER_create_with_size(2);
    if (result == NULL)
    {
        LogError("Failure creating BUFFER");
    }
    else
    {
        uint8_t* iterator = BUFFER_u_char(result);
        iterator[0] = DISCONNECT_TYPE;
        iterator[1] = 0;
    }
    return result;
}

BUFFER_HANDLE codec_v3_publish(MQTT_CODEC_HANDLE handle, QOS_VALUE qosValue, bool duplicateMsg, bool serverRetain, uint16_t packetId, const char* topicName, const uint8_t* msgBuffer, size_t buffLen)
{
    BUFFER_HANDLE result;
    /* Codes_SRS_MQTT_CODEC_07_005: [If the parameters topicName is NULL then mqtt_codec_publish shall return NULL.] */
    if (handle == NULL || topicName == NULL)
    {
        LogError("Invalid argument specified: topicName: %p, handle: %p", topicName, handle);
        result = NULL;
    }
    /* Codes_SRS_MQTT_CODEC_07_036: [mqtt_codec_publish shall return NULL if the buffLen variable is greater than the MAX_SEND_SIZE (0xFFFFFF7F).] */
    else if (buffLen > MAX_SEND_SIZE)
    {
        /* Codes_SRS_MQTT_CODEC_07_006: [If any error is encountered then mqtt_codec_publish shall return NULL.] */
        LogError("Publish buffer lengh is greater than max send size of %ul", MAX_SEND_SIZE);
        result = NULL;
    }
    else
    {
        PUBLISH_HEADER_INFO publishInfo ={ 0 };
        publishInfo.topicName = topicName;
        publishInfo.packetId = packetId;
        publishInfo.qualityOfServiceValue = qosValue;

        uint8_t headerFlags = 0;
        if (duplicateMsg) headerFlags |= PUBLISH_DUP_FLAG;
        if (serverRetain) headerFlags |= PUBLISH_QOS_RETAIN;
        if (qosValue != DELIVER_AT_MOST_ONCE)
        {
            if (qosValue == DELIVER_AT_LEAST_ONCE)
            {
                headerFlags |= PUBLISH_QOS_AT_LEAST_ONCE;
            }
            else
            {
                headerFlags |= PUBLISH_QOS_EXACTLY_ONCE;
            }
        }

        /* Codes_SRS_MQTT_CODEC_07_007: [mqtt_codec_publish shall return a BUFFER_HANDLE that represents a MQTT PUBLISH message.] */
        result = BUFFER_new();
        if (result == NULL)
        {
            LogError(BUFF_ALLOCATION_ERROR_MSG);
        }
        else
        {
            CODEC_V3_INSTANCE* mqtt_codec = (CODEC_V3_INSTANCE*)handle;

            if (mqtt_codec->trace_func != NULL)
            {
                mqtt_codec->trace_func(mqtt_codec->trace_ctx, "PUBLISH  | IS_DUP: %s | RETAIN: %d | QOS: %s", duplicateMsg ? TRUE_CONST : FALSE_CONST,
                    serverRetain ? 1 : 0,
                    retrieve_qos_value(publishInfo.qualityOfServiceValue) );
            }

            if (constructPublishVariableHeader(mqtt_codec, result, &publishInfo) != 0)
            {
                /* Codes_SRS_MQTT_CODEC_07_006: [If any error is encountered then mqtt_codec_publish shall return NULL.] */
                BUFFER_delete(result);
                result = NULL;
            }
            else
            {
                size_t payloadOffset = BUFFER_length(result);
                if (buffLen > 0)
                {
                    if (BUFFER_enlarge(result, buffLen) != 0)
                    {
                        /* Codes_SRS_MQTT_CODEC_07_006: [If any error is encountered then mqtt_codec_publish shall return NULL.] */
                        BUFFER_delete(result);
                        result = NULL;
                    }
                    else
                    {
                        uint8_t* iterator = BUFFER_u_char(result);
                        if (iterator == NULL)
                        {
                            /* Codes_SRS_MQTT_CODEC_07_006: [If any error is encountered then mqtt_codec_publish shall return NULL.] */
                            BUFFER_delete(result);
                            result = NULL;
                        }
                        else
                        {
                            iterator += payloadOffset;
                            // Write Message
                            (void)memcpy(iterator, msgBuffer, buffLen);
                            if (mqtt_codec->trace_func != NULL)
                            {
                                mqtt_codec->trace_func(mqtt_codec->trace_ctx, " | PAYLOAD_LEN: %zu", buffLen);
                            }
                        }
                    }
                }

                if (result != NULL)
                {
                    if (constructFixedHeader(result, PUBLISH_TYPE, headerFlags) != 0)
                    {
                        /* Codes_SRS_MQTT_CODEC_07_006: [If any error is encountered then mqtt_codec_publish shall return NULL.] */
                        BUFFER_delete(result);
                        result = NULL;
                    }
                }
            }
        }
    }
    return result;
}

BUFFER_HANDLE codec_v3_publishAck(uint16_t packetId)
{
    /* Codes_SRS_MQTT_CODEC_07_013: [On success mqtt_codec_publishAck shall return a BUFFER_HANDLE representation of a MQTT PUBACK packet.] */
    /* Codes_SRS_MQTT_CODEC_07_014 : [If any error is encountered then mqtt_codec_publishAck shall return NULL.] */
    return constructPublishReply(PUBACK_TYPE, 0, packetId);
}

BUFFER_HANDLE codec_v3_publishReceived(uint16_t packetId)
{
    /* Codes_SRS_MQTT_CODEC_07_015: [On success mqtt_codec_publishRecieved shall return a BUFFER_HANDLE representation of a MQTT PUBREC packet.] */
    /* Codes_SRS_MQTT_CODEC_07_016 : [If any error is encountered then mqtt_codec_publishRecieved shall return NULL.] */
    return constructPublishReply(PUBREC_TYPE, 0, packetId);
}

BUFFER_HANDLE codec_v3_publishRelease(uint16_t packetId)
{
    /* Codes_SRS_MQTT_CODEC_07_017: [On success mqtt_codec_publishRelease shall return a BUFFER_HANDLE representation of a MQTT PUBREL packet.] */
    /* Codes_SRS_MQTT_CODEC_07_018 : [If any error is encountered then mqtt_codec_publishRelease shall return NULL.] */
    return constructPublishReply(PUBREL_TYPE, 2, packetId);
}

BUFFER_HANDLE codec_v3_publishComplete(uint16_t packetId)
{
    /* Codes_SRS_MQTT_CODEC_07_019: [On success mqtt_codec_publishComplete shall return a BUFFER_HANDLE representation of a MQTT PUBCOMP packet.] */
    /* Codes_SRS_MQTT_CODEC_07_020 : [If any error is encountered then mqtt_codec_publishComplete shall return NULL.] */
    return constructPublishReply(PUBCOMP_TYPE, 0, packetId);
}

BUFFER_HANDLE codec_v3_ping(void)
{
    /* Codes_SRS_MQTT_CODEC_07_021: [On success mqtt_codec_ping shall construct a BUFFER_HANDLE that represents a MQTT PINGREQ packet.] */
    BUFFER_HANDLE result = BUFFER_create_with_size(2);
    if (result != NULL)
    {
        LogError(FAILURE_MSG_CREATE_BUFFER);
    }
    else
    {
        uint8_t* iterator = BUFFER_u_char(result);
        iterator[0] = PINGREQ_TYPE;
        iterator[1] = 0;
    }
    return result;
}

BUFFER_HANDLE codec_v3_subscribe(MQTT_CODEC_HANDLE handle, uint16_t packetId, SUBSCRIBE_PAYLOAD* subscribeList, size_t count)
{
    BUFFER_HANDLE result;
    /* Codes_SRS_MQTT_CODEC_07_023: [If the parameters subscribeList is NULL or if count is 0 then mqtt_codec_subscribe shall return NULL.] */
    if (handle == NULL || subscribeList == NULL || count == 0)
    {
        LogError("Invalid argument specified: handle: %p, subscribeList: %p, count: %ul", handle, subscribeList, (unsigned int)count);
        result = NULL;
    }
    else
    {
        CODEC_V3_INSTANCE* mqtt_codec = (CODEC_V3_INSTANCE*)handle;

        /* Codes_SRS_MQTT_CODEC_07_026: [mqtt_codec_subscribe shall return a BUFFER_HANDLE that represents a MQTT SUBSCRIBE message.]*/
        if ((result = BUFFER_new()) == NULL)
        {
            LogError(BUFF_ALLOCATION_ERROR_MSG);
        }
        else
        {
            if (mqtt_codec->trace_func != NULL)
            {
                mqtt_codec->trace_func(mqtt_codec->trace_ctx, "SUBSCRIBE | PACKET_ID: %"PRIu16, packetId);
            }

            if (constructSubscibeTypeVariableHeader(result, packetId) != 0)
            {
                /* Codes_SRS_MQTT_CODEC_07_025: [If any error is encountered then mqtt_codec_subscribe shall return NULL.] */
                BUFFER_delete(result);
                result = NULL;
            }
            /* Codes_SRS_MQTT_CODEC_07_024: [mqtt_codec_subscribe shall iterate through count items in the subscribeList.] */
            else if (addListItemsToSubscribePacket(mqtt_codec, result, subscribeList, count) != 0)
            {
                /* Codes_SRS_MQTT_CODEC_07_025: [If any error is encountered then mqtt_codec_subscribe shall return NULL.] */
                BUFFER_delete(result);
                result = NULL;
            }
            else if (constructFixedHeader(result, SUBSCRIBE_TYPE, SUBSCRIBE_FIXED_HEADER_FLAG) != 0)
            {
                /* Codes_SRS_MQTT_CODEC_07_025: [If any error is encountered then mqtt_codec_subscribe shall return NULL.] */
                BUFFER_delete(result);
                result = NULL;
            }
        }
    }
    return result;
}

BUFFER_HANDLE codec_v3_unsubscribe(MQTT_CODEC_HANDLE handle, uint16_t packetId, const char** unsubscribeList, size_t count)
{
    BUFFER_HANDLE result;
    /* Codes_SRS_MQTT_CODEC_07_027: [If the parameters unsubscribeList is NULL or if count is 0 then mqtt_codec_unsubscribe shall return NULL.] */
    if (handle == NULL || unsubscribeList == NULL || count == 0)
    {
        LogError("Invalid argument specified: handle: %p, subscribeList: %p, count: %ul", handle, unsubscribeList, (unsigned int)count);
        result = NULL;
    }
    else
    {
        CODEC_V3_INSTANCE* mqtt_codec = (CODEC_V3_INSTANCE*)handle;

        /* Codes_SRS_MQTT_CODEC_07_030: [mqtt_codec_unsubscribe shall return a BUFFER_HANDLE that represents a MQTT SUBSCRIBE message.] */
        if ((result = BUFFER_new()) != NULL)
        {
            LogError(BUFF_ALLOCATION_ERROR_MSG);
        }
        else if (constructSubscibeTypeVariableHeader(result, packetId) != 0)
        {
            /* Codes_SRS_MQTT_CODEC_07_029: [If any error is encountered then mqtt_codec_unsubscribe shall return NULL.] */
            BUFFER_delete(result);
            result = NULL;
        }
        else
        {
            if (mqtt_codec->trace_func != NULL)
            {
                mqtt_codec->trace_func(mqtt_codec->trace_ctx, "UNSUBSCRIBE | PACKET_ID: %"PRIu16, packetId);
            }
            /* Codes_SRS_MQTT_CODEC_07_028: [mqtt_codec_unsubscribe shall iterate through count items in the unsubscribeList.] */
            if (addListItemsToUnsubscribePacket(mqtt_codec, result, unsubscribeList, count) != 0)
            {
                /* Codes_SRS_MQTT_CODEC_07_029: [If any error is encountered then mqtt_codec_unsubscribe shall return NULL.] */
                BUFFER_delete(result);
                result = NULL;
            }
            else if (constructFixedHeader(result, UNSUBSCRIBE_TYPE, UNSUBSCRIBE_FIXED_HEADER_FLAG) != 0)
            {
                /* Codes_SRS_MQTT_CODEC_07_029: [If any error is encountered then mqtt_codec_unsubscribe shall return NULL.] */
                BUFFER_delete(result);
                result = NULL;
            }
        }
    }
    return result;
}

static void on_bytes_recv(void* context, const unsigned char* buffer, size_t size)
{
    CODEC_V3_INSTANCE* codec_data = (CODEC_V3_INSTANCE*)context;
    if (codec_data == NULL)
    {
    }
    else if (buffer == NULL || size == 0)
    {
        LogError("Invalid parameters buffer: %p, size: %ul", buffer, (unsigned int)size);
        codec_data->currPacket = PACKET_TYPE_ERROR;
        // Set Error callback
    }
    else
    {
        bool is_error = false;
        /* Codes_SRS_MQTT_CODEC_07_033: [mqtt_codec_bytesReceived constructs a sequence of bytes into the corresponding MQTT packets and on success returns zero.] */
        size_t index = 0;
        for (index = 0; index < size && !is_error; index++)
        {
            uint8_t iterator = ((int8_t*)buffer)[index];
            if (codec_data->codecState == CODEC_STATE_FIXED_HEADER)
            {
                if (codec_data->currPacket == UNKNOWN_TYPE)
                {
                    codec_data->currPacket = processControlPacketType(iterator, &codec_data->headerFlags);
                }
                else
                {
                    if (prepareheaderDataInfo(codec_data, iterator) != 0)
                    {
                        /* Codes_SRS_MQTT_CODEC_07_035: [If any error is encountered then the packet state will be marked as error and mqtt_codec_bytesReceived shall return a non-zero value.] */
                        codec_data->currPacket = PACKET_TYPE_ERROR;
                        is_error = true;
                    }
                    if (codec_data->currPacket == PINGRESP_TYPE)
                    {
                        /* Codes_SRS_MQTT_CODEC_07_034: [Upon a constructing a complete MQTT packet mqtt_codec_bytesReceived shall call the ON_PACKET_COMPLETE_CALLBACK function.] */
                        completePacketData(codec_data);
                    }
                }
            }
            else if (codec_data->codecState == CODEC_STATE_VAR_HEADER)
            {
                if (codec_data->headerData == NULL)
                {
                    codec_data->codecState = CODEC_STATE_PAYLOAD;
                }
                else
                {
                    uint8_t* dataBytes = BUFFER_u_char(codec_data->headerData);
                    if (dataBytes == NULL)
                    {
                        /* Codes_SRS_MQTT_CODEC_07_035: [If any error is encountered then the packet state will be marked as error and mqtt_codec_bytesReceived shall return a non-zero value.] */
                        codec_data->currPacket = PACKET_TYPE_ERROR;
                        is_error = true;
                    }
                    else
                    {
                        // Increment the data
                        dataBytes += codec_data->bufferOffset++;
                        *dataBytes = iterator;

                        size_t totalLen = BUFFER_length(codec_data->headerData);
                        if (codec_data->bufferOffset >= totalLen)
                        {
                            /* Codes_SRS_MQTT_CODEC_07_034: [Upon a constructing a complete MQTT packet mqtt_codec_bytesReceived shall call the ON_PACKET_COMPLETE_CALLBACK function.] */
                            completePacketData(codec_data);
                        }
                    }
                }
            }
            else
            {
                /* Codes_SRS_MQTT_CODEC_07_035: [If any error is encountered then the packet state will be marked as error and mqtt_codec_bytesReceived shall return a non-zero value.] */
                codec_data->currPacket = PACKET_TYPE_ERROR;
                is_error = true;
            }
        }
    }
}

int codec_v3_set_trace(MQTT_CODEC_HANDLE handle, TRACE_LOG_VALUE trace_func, void* trace_ctx)
{
    int result;
    if (handle == NULL)
    {
        result = __FAILURE__;
    }
    else
    {
        CODEC_V3_INSTANCE* codec_data = (CODEC_V3_INSTANCE*)handle;
        codec_data->trace_func = trace_func;
        codec_data->trace_ctx = trace_ctx;
        result = 0;
    }
    return result;
}

ON_BYTES_RECEIVED codec_v3_get_recv_func(void)
{
    return on_bytes_recv;
}
