// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

#include <stdlib.h>
#include <inttypes.h>
#include <stdarg.h>
#include <stdbool.h>
#include "azure_c_shared_utility/agenttime.h"
#include "azure_c_shared_utility/const_defines.h"
#include "azure_c_shared_utility/crt_abstractions.h"
#include "azure_c_shared_utility/gballoc.h"
#include "azure_c_shared_utility/optimize_size.h"
#include "azure_macro_utils/macro_utils.h"
#include "azure_c_shared_utility/platform.h"
#include "azure_c_shared_utility/strings.h"
#include "azure_c_shared_utility/threadapi.h"
#include "azure_c_shared_utility/tickcounter.h"
#include "azure_c_shared_utility/xlogging.h"

#include "azure_umqtt_c/mqtt_client.h"
#include "azure_umqtt_c/mqtt_codec_v3.h"

#define VARIABLE_HEADER_OFFSET          2
#define RETAIN_FLAG_MASK                0x1
#define QOS_LEAST_ONCE_FLAG_MASK        0x2
#define QOS_EXACTLY_ONCE_FLAG_MASK      0x4
#define DUPLICATE_FLAG_MASK             0x8
#define CONNECT_PACKET_MASK             0xf0
#define TIME_MAX_BUFFER                 16
#define DEFAULT_MAX_PING_RESPONSE_TIME  80  // % of time to send pings
#define MAX_CLOSE_RETRIES               20

MU_DEFINE_ENUM_STRINGS(QOS_VALUE, QOS_VALUE_VALUES);

#define MQTT_STATUS_INITIAL_STATUS      0x0000
#define MQTT_STATUS_CLIENT_CONNECTED    0x0001
#define MQTT_STATUS_SOCKET_CONNECTED    0x0002
#define MQTT_STATUS_PENDING_CLOSE       0x0004

#define MQTT_FLAGS_LOG_TRACE           0x0001
#define MQTT_FLAGS_RAW_TRACE           0x0002

typedef struct MQTT_CLIENT_TAG
{
    XIO_HANDLE xioHandle;

    MQTT_CODEC_V3_HANDLE codec_handle;
    char* trace_line;
    size_t trace_alloc;

    CONTROL_PACKET_TYPE packetState;
    TICK_COUNTER_HANDLE packetTickCntr;
    tickcounter_ms_t packetSendTimeMs;
    ON_MQTT_OPERATION_CALLBACK fnOperationCallback;
    ON_MQTT_MESSAGE_RECV_CALLBACK fnMessageRecv;
    void* ctx;
    ON_MQTT_ERROR_CALLBACK fnOnErrorCallBack;
    void* errorCBCtx;
    ON_MQTT_DISCONNECTED_CALLBACK disconnect_cb;
    void* disconnect_ctx;
    QOS_VALUE qosValue;
    uint16_t keepAliveInterval;
    MQTT_CLIENT_OPTIONS mqttOptions;

    uint16_t mqtt_status;
    uint16_t mqtt_flags;

    tickcounter_ms_t timeSincePing;
    uint16_t maxPingRespTime;
} MQTT_CLIENT;

static bool is_trace_enabled(MQTT_CLIENT* mqtt_client)
{
    return (mqtt_client->mqtt_flags & MQTT_FLAGS_LOG_TRACE);
}

#ifdef ENABLE_RAW_TRACE
static bool is_raw_trace_enabled(MQTT_CLIENT* mqtt_client)
{
    return (mqtt_client->mqtt_flags & MQTT_FLAGS_RAW_TRACE);
}
#endif // ENABLE_RAW_TRACE

static void on_connection_closed(void* context)
{
    MQTT_CLIENT* mqtt_client = (MQTT_CLIENT*)context;
    if (mqtt_client != NULL)
    {
        mqtt_client->mqtt_status &= ~MQTT_STATUS_SOCKET_CONNECTED;
        if (mqtt_client->disconnect_cb)
        {
            mqtt_client->disconnect_cb(mqtt_client->disconnect_ctx);
        }
    }
}

static void close_connection(MQTT_CLIENT* mqtt_client)
{
    if (mqtt_client->mqtt_status & MQTT_STATUS_SOCKET_CONNECTED)
    {
        (void)xio_close(mqtt_client->xioHandle, on_connection_closed, mqtt_client);
        if (mqtt_client->disconnect_cb == NULL)
        {
            size_t counter = 0;
            do
            {
                xio_dowork(mqtt_client->xioHandle);
                counter++;
                ThreadAPI_Sleep(2);
            } while (mqtt_client->mqtt_status & MQTT_STATUS_SOCKET_CONNECTED && counter < MAX_CLOSE_RETRIES);
        }
        // Clear the handle because we don't use it anymore
        mqtt_client->xioHandle = NULL;
    }
    else
    {
        mqtt_client->mqtt_status &= ~MQTT_STATUS_SOCKET_CONNECTED;
        if (mqtt_client->disconnect_cb)
        {
            mqtt_client->disconnect_cb(mqtt_client->disconnect_ctx);
        }
    }
}

static void set_error_callback(MQTT_CLIENT* mqtt_client, MQTT_CLIENT_EVENT_ERROR error_type)
{
    if (mqtt_client->fnOnErrorCallBack)
    {
        mqtt_client->fnOnErrorCallBack(mqtt_client, error_type, mqtt_client->errorCBCtx);
    }
    close_connection(mqtt_client);
}

static STRING_HANDLE construct_trace_log_handle(MQTT_CLIENT* mqtt_client)
{
    STRING_HANDLE trace_log;
    if (is_trace_enabled(mqtt_client) )
    {
        trace_log = STRING_new();
    }
    else
    {
        trace_log = NULL;
    }
    return trace_log;
}

static uint16_t byteutil_read_uint16(uint8_t** buffer, size_t byteLen)
{
    uint16_t result = 0;
    if (buffer != NULL && *buffer != NULL && byteLen >= 2)
    {
        result = 256 * (**buffer) + (*(*buffer + 1));
        *buffer += 2; // Move the ptr
    }
    else
    {
        LogError("byteutil_read_uint16 == NULL or less than 2");
    }
    return result;
}

static char* byteutil_readUTF(uint8_t** buffer, size_t* byteLen)
{
    char* result = NULL;

    const uint8_t* bufferInitial = *buffer;
    // Get the length of the string
    uint16_t stringLen = byteutil_read_uint16(buffer, *byteLen);
    // Verify that byteutil_read_uint16 succeeded (by stringLen>0) and that we're
    // not being asked to read a string longer than buffer passed in.
    if ((stringLen > 0) && ((size_t)(stringLen + (*buffer - bufferInitial)) <= *byteLen))
    {
        result = (char*)malloc(stringLen + 1);
        if (result != NULL)
        {
            (void)memcpy(result, *buffer, stringLen);
            result[stringLen] = '\0';
            *buffer += stringLen;
            *byteLen = stringLen;
         }
    }
    else
    {
        LogError("String passed not a valid UTF.");
    }

    return result;
}

static uint8_t byteutil_readByte(uint8_t** buffer)
{
    uint8_t result = 0;
    if (buffer != NULL)
    {
        result = **buffer;
        (*buffer)++;
    }
    else
    {
        LogError("readByte buffer == NULL.");
    }
    return result;
}

static void send_complete(void* context, IO_SEND_RESULT send_result)
{
    MQTT_CLIENT* mqtt_client = (MQTT_CLIENT*)context;
    if (mqtt_client != NULL)
    {
        if (send_result == IO_SEND_OK)
        {
            if (mqtt_client->packetState == DISCONNECT_TYPE)
            {
                /*Codes_SRS_MQTT_CLIENT_07_032: [If the actionResult parameter is of type MQTT_CLIENT_ON_DISCONNECT the the msgInfo value shall be NULL.]*/
                if (mqtt_client->fnOperationCallback != NULL)
                {
                    mqtt_client->fnOperationCallback(mqtt_client, MQTT_CLIENT_ON_DISCONNECT, NULL, mqtt_client->ctx);
                }

                // Mark to close
                mqtt_client->mqtt_status |= MQTT_STATUS_PENDING_CLOSE;
            }
        }
        else if (send_result == IO_SEND_ERROR)
        {
            LogError("MQTT Send Complete Failure send_result: %d", (int)send_result);
            set_error_callback(mqtt_client, MQTT_CLIENT_COMMUNICATION_ERROR);
        }
    }
    else
    {
        LogError("MQTT Send Complete Failure with NULL mqtt_client");
    }
}

#ifndef NO_LOGGING
static void get_log_time(char* timeResult, size_t len)
{
    if (timeResult != NULL)
    {
        time_t agent_time = get_time(NULL);
        if (agent_time == (time_t)-1)
        {
            timeResult[0] = '\0';
        }
        else
        {
            struct tm* tmInfo = localtime(&agent_time);
            if (tmInfo == NULL)
            {
                timeResult[0] = '\0';
            }
            else
            {
                if (strftime(timeResult, len, "%H:%M:%S", tmInfo) == 0)
                {
                    timeResult[0] = '\0';
                }
            }
        }
    }
}

#ifdef ENABLE_RAW_TRACE
static const char* retrieve_packet_type(CONTROL_PACKET_TYPE packet)
{
    switch (packet&CONNECT_PACKET_MASK)
    {
        case CONNECT_TYPE: return "CONNECT";
        case CONNACK_TYPE:  return "CONNACK";
        case PUBLISH_TYPE:  return "PUBLISH";
        case PUBACK_TYPE:  return "PUBACK";
        case PUBREC_TYPE:  return "PUBREC";
        case PUBREL_TYPE:  return "PUBREL";
        case SUBSCRIBE_TYPE:  return "SUBSCRIBE";
        case SUBACK_TYPE:  return "SUBACK";
        case UNSUBSCRIBE_TYPE:  return "UNSUBSCRIBE";
        case UNSUBACK_TYPE:  return "UNSUBACK";
        case PINGREQ_TYPE:  return "PINGREQ";
        case PINGRESP_TYPE:  return "PINGRESP";
        case DISCONNECT_TYPE:  return "DISCONNECT";
        default:
        case PACKET_TYPE_ERROR:
        case UNKNOWN_TYPE:
            return "UNKNOWN";
    }
}

static void log_outgoing_raw_trace(MQTT_CLIENT* mqtt_client, const uint8_t* data, size_t length)
{
    if (mqtt_client != NULL && data != NULL && length > 0 && is_raw_trace_enabled(mqtt_client))
    {
        char tmBuffer[TIME_MAX_BUFFER];
        get_log_time(tmBuffer, TIME_MAX_BUFFER);

        LOG(AZ_LOG_TRACE, 0, "-> %s %s: ", tmBuffer, retrieve_packet_type((unsigned char)data[0]));
        size_t index = 0;
        for (index = 0; index < length; index++)
        {
            LOG(AZ_LOG_TRACE, 0, "0x%02x ", data[index]);
        }
        LOG(AZ_LOG_TRACE, LOG_LINE, " ");
    }
}

static void log_incoming_raw_trace(MQTT_CLIENT* mqtt_client, CONTROL_PACKET_TYPE packet, uint8_t flags, const uint8_t* data, size_t length)
{
    if (mqtt_client != NULL && is_raw_trace_enabled(mqtt_client))
    {
        if (data != NULL && length > 0)
        {
            char tmBuffer[TIME_MAX_BUFFER];
            get_log_time(tmBuffer, TIME_MAX_BUFFER);

            LOG(AZ_LOG_TRACE, 0, "<- %s %s: 0x%02x 0x%02x ", tmBuffer, retrieve_packet_type((CONTROL_PACKET_TYPE)packet), (unsigned int)(packet | flags), (unsigned int)length);
            size_t index = 0;
            for (index = 0; index < length; index++)
            {
                LOG(AZ_LOG_TRACE, 0, "0x%02x ", data[index]);
            }
            LOG(AZ_LOG_TRACE, LOG_LINE, " ");
        }
        else if (packet == PINGRESP_TYPE)
        {
            char tmBuffer[TIME_MAX_BUFFER];
            get_log_time(tmBuffer, TIME_MAX_BUFFER);
            LOG(AZ_LOG_TRACE, LOG_LINE, "<- %s %s: 0x%02x 0x%02x ", tmBuffer, retrieve_packet_type((CONTROL_PACKET_TYPE)packet), (unsigned int)(packet | flags), (unsigned int)length);
        }
    }
}
#endif // ENABLE_RAW_TRACE

static void log_outgoing_trace(MQTT_CLIENT* mqtt_client, const char* trace_log)
{
    if (mqtt_client != NULL && is_trace_enabled(mqtt_client) && trace_log != NULL)
    {
        char tmBuffer[TIME_MAX_BUFFER];
        get_log_time(tmBuffer, TIME_MAX_BUFFER);
        LOG(AZ_LOG_TRACE, LOG_LINE, "-> %s %s", tmBuffer, trace_log);
    }
}

static void log_incoming_trace(MQTT_CLIENT* mqtt_client, const char* trace_log)
{
    if (mqtt_client != NULL && is_trace_enabled(mqtt_client) && trace_log != NULL)
    {
        char tmBuffer[TIME_MAX_BUFFER];
        get_log_time(tmBuffer, TIME_MAX_BUFFER);
        LOG(AZ_LOG_TRACE, LOG_LINE, "<- %s %s", tmBuffer, trace_log);
    }
}
#else // NO_LOGGING
static void log_outgoing_raw_trace(MQTT_CLIENT* mqtt_client, const uint8_t* data, size_t length)
{
    AZURE_UNREFERENCED_PARAMETER(mqtt_client);
    AZURE_UNREFERENCED_PARAMETER(data);
    AZURE_UNREFERENCED_PARAMETER(length);
}

static void log_outgoing_trace(MQTT_CLIENT* mqtt_client, STRING_HANDLE trace_log)
{
    AZURE_UNREFERENCED_PARAMETER(mqtt_client);
    AZURE_UNREFERENCED_PARAMETER(trace_log);
}

static void log_incoming_trace(MQTT_CLIENT* mqtt_client, STRING_HANDLE trace_log)
{
    AZURE_UNREFERENCED_PARAMETER(mqtt_client);
    AZURE_UNREFERENCED_PARAMETER(trace_log);
}

static void log_incoming_raw_trace(MQTT_CLIENT* mqtt_client, CONTROL_PACKET_TYPE packet, uint8_t flags, const uint8_t* data, size_t length)
{
    AZURE_UNREFERENCED_PARAMETER(mqtt_client);
    AZURE_UNREFERENCED_PARAMETER(packet);
    AZURE_UNREFERENCED_PARAMETER(flags);
    AZURE_UNREFERENCED_PARAMETER(data);
    AZURE_UNREFERENCED_PARAMETER(length);
}
#endif // NO_LOGGING

static void flush_trace_log(MQTT_CLIENT* mqtt_client)
{
    if (is_trace_enabled(mqtt_client))
    {
        log_outgoing_trace(mqtt_client, mqtt_client->trace_line);
        free(mqtt_client->trace_line);
        mqtt_client->trace_line = NULL;
        mqtt_client->trace_alloc = 0;
    }
}

static int send_packet_msg(MQTT_CLIENT* mqtt_client, const unsigned char* data, size_t length)
{
    int result;

    if (tickcounter_get_current_ms(mqtt_client->packetTickCntr, &mqtt_client->packetSendTimeMs) != 0)
    {
        LogError("Failure getting current ms tickcounter");
        result = MU_FAILURE;
    }
    else
    {
        result = xio_send(mqtt_client->xioHandle, (const void*)data, length, send_complete, mqtt_client);
        if (result != 0)
        {
            LogError("Failure sending control packet data");
            result = MU_FAILURE;
        }
        else
        {
#ifdef ENABLE_RAW_TRACE
            log_outgoing_raw_trace(mqtt_client, (const uint8_t*)data, length);
#endif
        }
    }
    return result;
}

static void on_open_complete(void* context, IO_OPEN_RESULT open_result)
{
    MQTT_CLIENT* mqtt_client = (MQTT_CLIENT*)context;
    if (mqtt_client != NULL)
    {
        if (open_result == IO_OPEN_OK && !(mqtt_client->mqtt_status & MQTT_STATUS_SOCKET_CONNECTED))
        {
            mqtt_client->packetState = CONNECT_TYPE;
            mqtt_client->mqtt_status |= MQTT_STATUS_SOCKET_CONNECTED;

            // Send the Connect packet
            BUFFER_HANDLE connPacket = codec_v3_connect(mqtt_client->codec_handle, &mqtt_client->mqttOptions);
            if (connPacket == NULL)
            {
                LogError("Error: mqtt_codec_connect failed");
            }
            else
            {
                size_t size = BUFFER_length(connPacket);
                /*Codes_SRS_MQTT_CLIENT_07_009: [On success mqtt_client_connect shall send the MQTT CONNECT to the endpoint.]*/
                if (send_packet_msg(mqtt_client, BUFFER_u_char(connPacket), size) != 0)
                {
                    LogError("Error: failure sending connect packet");
                    // Set the status to pending close because we connot continue
                    // with CONN failing to send
                    if (mqtt_client->fnOnErrorCallBack)
                    {
                        mqtt_client->fnOnErrorCallBack(mqtt_client, MQTT_CLIENT_CONNECTION_ERROR, mqtt_client->errorCBCtx);
                    }
                    mqtt_client->mqtt_status |= MQTT_STATUS_PENDING_CLOSE;
                }
                else
                {
                    flush_trace_log(mqtt_client);
                }
                BUFFER_delete(connPacket);
            }
        }
        else
        {
            LogError("Error: failure opening connection to endpoint");
            if (!(mqtt_client->mqtt_status & MQTT_STATUS_SOCKET_CONNECTED) && mqtt_client->fnOnErrorCallBack)
            {
                mqtt_client->fnOnErrorCallBack(mqtt_client, MQTT_CLIENT_CONNECTION_ERROR, mqtt_client->errorCBCtx);
            }
            mqtt_client->mqtt_status |= MQTT_STATUS_PENDING_CLOSE;
        }
    }
    else
    {
        LogError("Error: mqtt_client is NULL");
    }
}

static void on_io_error(void* context)
{
    MQTT_CLIENT* mqtt_client = (MQTT_CLIENT*)context;
    if (mqtt_client != NULL && mqtt_client->fnOperationCallback)
    {
        /*Codes_SRS_MQTT_CLIENT_07_032: [If the actionResult parameter is of type MQTT_CLIENT_ON_DISCONNECT the the msgInfo value shall be NULL.]*/
        /* Codes_SRS_MQTT_CLIENT_07_036: [ If an error is encountered by the ioHandle the mqtt_client shall call xio_close. ] */
        set_error_callback(mqtt_client, MQTT_CLIENT_CONNECTION_ERROR);
    }
    else
    {
        LogError("Error invalid parameter: mqtt_client: %p", mqtt_client);
    }
}

static void clear_mqtt_options(MQTT_CLIENT* mqtt_client)
{
    if (mqtt_client->mqttOptions.clientId != NULL)
    {
        free(mqtt_client->mqttOptions.clientId);
        mqtt_client->mqttOptions.clientId = NULL;
    }

    if (mqtt_client->mqttOptions.willTopic != NULL)
    {
        free(mqtt_client->mqttOptions.willTopic);
        mqtt_client->mqttOptions.willTopic = NULL;
    }

    if (mqtt_client->mqttOptions.willMessage != NULL)
    {
        free(mqtt_client->mqttOptions.willMessage);
        mqtt_client->mqttOptions.willMessage = NULL;
    }

    if (mqtt_client->mqttOptions.username != NULL)
    {
        free(mqtt_client->mqttOptions.username);
        mqtt_client->mqttOptions.username = NULL;
    }

    if (mqtt_client->mqttOptions.password != NULL)
    {
        free(mqtt_client->mqttOptions.password);
        mqtt_client->mqttOptions.password = NULL;
    }
}

static int clone_mqtt_options(MQTT_CLIENT* mqtt_client, const MQTT_CLIENT_OPTIONS* mqttOptions)
{
    int result = 0;
    char* temp_option;

    if (mqttOptions->clientId != NULL)
    {
        if (mallocAndStrcpy_s(&temp_option, mqttOptions->clientId) != 0)
        {
            result = MU_FAILURE;
            LogError("mallocAndStrcpy_s clientId");
        }
        else
        {
            if (mqtt_client->mqttOptions.clientId != NULL)
            {
                free(mqtt_client->mqttOptions.clientId);
            }
            mqtt_client->mqttOptions.clientId = temp_option;
        }
    }
    if (result == 0 && mqttOptions->willTopic != NULL)
    {
        temp_option = NULL;
        if (mallocAndStrcpy_s(&temp_option, mqttOptions->willTopic) != 0)
        {
            result = MU_FAILURE;
            LogError("mallocAndStrcpy_s willTopic");
        }
        else
        {
            if (mqtt_client->mqttOptions.willTopic != NULL)
            {
                free(mqtt_client->mqttOptions.willTopic);
            }
            mqtt_client->mqttOptions.willTopic = temp_option;
        }
    }
    if (result == 0 && mqttOptions->willMessage != NULL)
    {
        temp_option = NULL;
        if (mallocAndStrcpy_s(&temp_option, mqttOptions->willMessage) != 0)
        {
            LogError("mallocAndStrcpy_s willMessage");
            result = MU_FAILURE;
        }
        else
        {
            if (mqtt_client->mqttOptions.willMessage != NULL)
            {
                free(mqtt_client->mqttOptions.willMessage);
            }
            mqtt_client->mqttOptions.willMessage = temp_option;
        }
    }
    if (result == 0 && mqttOptions->username != NULL)
    {
        temp_option = NULL;
        if (mallocAndStrcpy_s(&temp_option, mqttOptions->username) != 0)
        {
            LogError("mallocAndStrcpy_s username");
            result = MU_FAILURE;
        }
        else
        {
            if (mqtt_client->mqttOptions.username != NULL)
            {
                free(mqtt_client->mqttOptions.username);
            }
            mqtt_client->mqttOptions.username = temp_option;
        }
    }
    if (result == 0 && mqttOptions->password != NULL)
    {
        temp_option = NULL;
        if (mallocAndStrcpy_s(&temp_option, mqttOptions->password) != 0)
        {
            LogError("mallocAndStrcpy_s password");
            result = MU_FAILURE;
        }
        else
        {
            if (mqtt_client->mqttOptions.password != NULL)
            {
                free(mqtt_client->mqttOptions.password);
            }
            mqtt_client->mqttOptions.password = temp_option;
        }
    }
    if (result == 0)
    {
        mqtt_client->mqttOptions.keepAliveInterval = mqttOptions->keepAliveInterval;
        mqtt_client->mqttOptions.messageRetain = mqttOptions->messageRetain;
        mqtt_client->mqttOptions.useCleanSession = mqttOptions->useCleanSession;
        mqtt_client->mqttOptions.qualityOfServiceValue = mqttOptions->qualityOfServiceValue;
    }
    else
    {
        clear_mqtt_options(mqtt_client);
    }
    return result;
}

static void process_publish_msg(MQTT_CLIENT* mqtt_client, uint8_t* initialPos, size_t packetLength, int flags)
{
    bool isDuplicateMsg = (flags & DUPLICATE_FLAG_MASK) ? true : false;
    bool isRetainMsg = (flags & RETAIN_FLAG_MASK) ? true : false;
    QOS_VALUE qosValue = (flags == 0) ? DELIVER_AT_MOST_ONCE : (flags & QOS_LEAST_ONCE_FLAG_MASK) ? DELIVER_AT_LEAST_ONCE : DELIVER_EXACTLY_ONCE;

    uint8_t* iterator = initialPos;
    size_t numberOfBytesToBeRead = packetLength;
    size_t lengthOfTopicName = numberOfBytesToBeRead;
    char* topicName = byteutil_readUTF(&iterator, &lengthOfTopicName);
    if (topicName == NULL)
    {
        LogError("Publish MSG: failure reading topic name");
        set_error_callback(mqtt_client, MQTT_CLIENT_PARSE_ERROR);
    }
    else
    {
        STRING_HANDLE trace_log = NULL;

#ifndef NO_LOGGING
        if (is_trace_enabled(mqtt_client))
        {
            trace_log = STRING_construct_sprintf("PUBLISH | IS_DUP: %s | RETAIN: %d | QOS: %s | TOPIC_NAME: %s", isDuplicateMsg ? TRUE_CONST : FALSE_CONST,
                isRetainMsg ? 1 : 0, MU_ENUM_TO_STRING(QOS_VALUE, qosValue), topicName);
        }
#endif
        uint16_t packetId = 0;
        numberOfBytesToBeRead = packetLength - (iterator - initialPos);
        if (qosValue != DELIVER_AT_MOST_ONCE)
        {
            packetId = byteutil_read_uint16(&iterator, numberOfBytesToBeRead);
#ifndef NO_LOGGING
            if (is_trace_enabled(mqtt_client))
            {
                STRING_sprintf(trace_log, " | PACKET_ID: %"PRIu16, packetId);
            }
#endif
        }
        if ((qosValue != DELIVER_AT_MOST_ONCE) && (packetId == 0))
        {
            LogError("Publish MSG: packetId=0, invalid");
            set_error_callback(mqtt_client, MQTT_CLIENT_PARSE_ERROR);
        }
        else
        {
            numberOfBytesToBeRead = packetLength - (iterator - initialPos);

            MQTT_MESSAGE_HANDLE msgHandle = mqttmessage_create_in_place(packetId, topicName, qosValue, iterator, numberOfBytesToBeRead);
            if (msgHandle == NULL)
            {
                LogError("failure in mqttmessage_create");
                set_error_callback(mqtt_client, MQTT_CLIENT_MEMORY_ERROR);
            }
            else if (mqttmessage_setIsDuplicateMsg(msgHandle, isDuplicateMsg) != 0 ||
                     mqttmessage_setIsRetained(msgHandle, isRetainMsg) != 0)
            {
                LogError("failure setting mqtt message property");
                set_error_callback(mqtt_client, MQTT_CLIENT_MEMORY_ERROR);
            }
            else
            {
#ifndef NO_LOGGING
                if (is_trace_enabled(mqtt_client))
                {
                    STRING_sprintf(trace_log, " | PAYLOAD_LEN: %lu", (unsigned long)numberOfBytesToBeRead);
                    log_incoming_trace(mqtt_client, STRING_c_str(trace_log));
                }
#endif
                mqtt_client->fnMessageRecv(msgHandle, mqtt_client->ctx);

                BUFFER_HANDLE pubRel = NULL;
                if (qosValue == DELIVER_EXACTLY_ONCE)
                {
                    pubRel = codec_v3_publishReceived(packetId);
                    if (pubRel == NULL)
                    {
                        LogError("Failed to allocate publish receive message.");
                        set_error_callback(mqtt_client, MQTT_CLIENT_MEMORY_ERROR);
                    }
                }
                else if (qosValue == DELIVER_AT_LEAST_ONCE)
                {
                    pubRel = codec_v3_publishAck(packetId);
                    if (pubRel == NULL)
                    {
                        LogError("Failed to allocate publish ack message.");
                        set_error_callback(mqtt_client, MQTT_CLIENT_MEMORY_ERROR);
                    }
                }
                if (pubRel != NULL)
                {
                    size_t size = BUFFER_length(pubRel);
                    if (send_packet_msg(mqtt_client, BUFFER_u_char(pubRel), size) != 0)
                    {
                        LogError("Failed sending publish reply.");
                        set_error_callback(mqtt_client, MQTT_CLIENT_COMMUNICATION_ERROR);
                    }
                    BUFFER_delete(pubRel);
                }
            }
            mqttmessage_destroy(msgHandle);
        }

        if (trace_log != NULL)
        {
            STRING_delete(trace_log);
        }

        free(topicName);
    }
}

static void recv_complete_callback(void* context, CONTROL_PACKET_TYPE packet, int flags, BUFFER_HANDLE headerData)
{
    MQTT_CLIENT* mqtt_client = (MQTT_CLIENT*)context;
    if (mqtt_client != NULL)
    {
        size_t packetLength = 0;
        uint8_t* iterator = NULL;
        if (headerData != NULL)
        {
            packetLength = BUFFER_length(headerData);
            iterator = BUFFER_u_char(headerData);
        }

#ifdef ENABLE_RAW_TRACE
        log_incoming_raw_trace(mqtt_client, packet, (uint8_t)flags, iterator, packetLength);
#endif
        if ((iterator != NULL && packetLength > 0) || packet == PINGRESP_TYPE)
        {
            switch (packet)
            {
                case CONNACK_TYPE:
                {
                    /*Codes_SRS_MQTT_CLIENT_07_028: [If the actionResult parameter is of type CONNECT_ACK then the msgInfo value shall be a CONNECT_ACK structure.]*/
                    CONNECT_ACK connack = { 0 };
                    connack.isSessionPresent = (byteutil_readByte(&iterator) == 0x1) ? true : false;
                    uint8_t rc = byteutil_readByte(&iterator);
                    connack.returnCode =
                        (rc < ((uint8_t)CONN_REFUSED_UNKNOWN)) ?
                        (CONNECT_RETURN_CODE)rc : CONN_REFUSED_UNKNOWN;

#ifndef NO_LOGGING
                    if (is_trace_enabled(mqtt_client))
                    {
                        STRING_HANDLE trace_log = STRING_construct_sprintf("CONNACK | SESSION_PRESENT: %s | RETURN_CODE: 0x%x", connack.isSessionPresent ? TRUE_CONST : FALSE_CONST, connack.returnCode);
                        log_incoming_trace(mqtt_client, STRING_c_str(trace_log));
                        STRING_delete(trace_log);
                    }
#endif
                    mqtt_client->fnOperationCallback(mqtt_client, MQTT_CLIENT_ON_CONNACK, (void*)&connack, mqtt_client->ctx);

                    if (connack.returnCode == CONNECTION_ACCEPTED)
                    {
                        mqtt_client->mqtt_status |= MQTT_STATUS_CLIENT_CONNECTED;
                    }
                    break;
                }
                case PUBLISH_TYPE:
                {
                    process_publish_msg(mqtt_client, iterator, packetLength, flags);
                    break;
                }
                case PUBACK_TYPE:
                case PUBREC_TYPE:
                case PUBREL_TYPE:
                case PUBCOMP_TYPE:
                {
                    /*Codes_SRS_MQTT_CLIENT_07_029: [If the actionResult parameter are of types PUBACK_TYPE, PUBREC_TYPE, PUBREL_TYPE or PUBCOMP_TYPE then the msgInfo value shall be a PUBLISH_ACK structure.]*/
                    MQTT_CLIENT_EVENT_RESULT action = (packet == PUBACK_TYPE) ? MQTT_CLIENT_ON_PUBLISH_ACK :
                        (packet == PUBREC_TYPE) ? MQTT_CLIENT_ON_PUBLISH_RECV :
                        (packet == PUBREL_TYPE) ? MQTT_CLIENT_ON_PUBLISH_REL : MQTT_CLIENT_ON_PUBLISH_COMP;

                    PUBLISH_ACK publish_ack = { 0 };
                    publish_ack.packetId = byteutil_read_uint16(&iterator, packetLength);

#ifndef NO_LOGGING
                    if (is_trace_enabled(mqtt_client))
                    {
                        STRING_HANDLE trace_log = STRING_construct_sprintf("%s | PACKET_ID: %"PRIu16, packet == PUBACK_TYPE ? "PUBACK" : (packet == PUBREC_TYPE) ? "PUBREC" : (packet == PUBREL_TYPE) ? "PUBREL" : "PUBCOMP",
                            publish_ack.packetId);
                        log_incoming_trace(mqtt_client, STRING_c_str(trace_log));
                        STRING_delete(trace_log);
                    }
#endif
                    BUFFER_HANDLE pubRel = NULL;
                    mqtt_client->fnOperationCallback(mqtt_client, action, (void*)&publish_ack, mqtt_client->ctx);
                    if (packet == PUBREC_TYPE)
                    {
                        pubRel = codec_v3_publishRelease(publish_ack.packetId);
                        if (pubRel == NULL)
                        {
                            LogError("Failed to allocate publish release message.");
                            set_error_callback(mqtt_client, MQTT_CLIENT_MEMORY_ERROR);
                        }
                    }
                    else if (packet == PUBREL_TYPE)
                    {
                        pubRel = codec_v3_publishComplete(publish_ack.packetId);
                        if (pubRel == NULL)
                        {
                            LogError("Failed to allocate publish complete message.");
                            set_error_callback(mqtt_client, MQTT_CLIENT_MEMORY_ERROR);
                        }
                    }
                    if (pubRel != NULL)
                    {
                        size_t size = BUFFER_length(pubRel);
                        if (send_packet_msg(mqtt_client, BUFFER_u_char(pubRel), size) != 0)
                        {
                            LogError("Failed sending publish reply.");
                            set_error_callback(mqtt_client, MQTT_CLIENT_COMMUNICATION_ERROR);
                        }
                        BUFFER_delete(pubRel);
                    }
                    break;
                }
                case SUBACK_TYPE:
                {

                    /*Codes_SRS_MQTT_CLIENT_07_030: [If the actionResult parameter is of type SUBACK_TYPE then the msgInfo value shall be a SUBSCRIBE_ACK structure.]*/
                    SUBSCRIBE_ACK suback = { 0 };

                    size_t remainLen = packetLength;
                    suback.packetId = byteutil_read_uint16(&iterator, packetLength);
                    remainLen -= 2;

#ifndef NO_LOGGING
                    STRING_HANDLE trace_log = NULL;
                    if (is_trace_enabled(mqtt_client))
                    {
                        trace_log = STRING_construct_sprintf("SUBACK | PACKET_ID: %"PRIu16, suback.packetId);
                    }
#endif
                    // Allocate the remaining len
                    suback.qosReturn = (QOS_VALUE*)malloc(sizeof(QOS_VALUE)*remainLen);
                    if (suback.qosReturn != NULL)
                    {
                        while (remainLen > 0)
                        {
                            uint8_t qosRet = byteutil_readByte(&iterator);
                            suback.qosReturn[suback.qosCount++] =
                                (qosRet <= ((uint8_t)DELIVER_EXACTLY_ONCE)) ?
                                (QOS_VALUE)qosRet :  DELIVER_FAILURE;
                            remainLen--;
#ifndef NO_LOGGING
                            if (is_trace_enabled(mqtt_client))
                            {
                                STRING_sprintf(trace_log, " | RETURN_CODE: %"PRIu16, suback.qosReturn[suback.qosCount-1]);
                            }
#endif
                        }

#ifndef NO_LOGGING
                        if (is_trace_enabled(mqtt_client))
                        {
                            log_incoming_trace(mqtt_client, STRING_c_str(trace_log));
                            STRING_delete(trace_log);
                        }
#endif
                        mqtt_client->fnOperationCallback(mqtt_client, MQTT_CLIENT_ON_SUBSCRIBE_ACK, (void*)&suback, mqtt_client->ctx);
                        free(suback.qosReturn);
                    }
                    else
                    {
                        LogError("allocation of quality of service value failed.");
                        set_error_callback(mqtt_client, MQTT_CLIENT_MEMORY_ERROR);
                    }
                    break;
                }
                case UNSUBACK_TYPE:
                {
                    /*Codes_SRS_MQTT_CLIENT_07_031: [If the actionResult parameter is of type UNSUBACK_TYPE then the msgInfo value shall be a UNSUBSCRIBE_ACK structure.]*/
                    UNSUBSCRIBE_ACK unsuback = { 0 };
                    unsuback.packetId = byteutil_read_uint16(&iterator, packetLength);

#ifndef NO_LOGGING
                    if (is_trace_enabled(mqtt_client))
                    {
                        STRING_HANDLE trace_log = STRING_construct_sprintf("UNSUBACK | PACKET_ID: %"PRIu16, unsuback.packetId);
                        log_incoming_trace(mqtt_client, STRING_c_str(trace_log));
                        STRING_delete(trace_log);
                    }
#endif
                    mqtt_client->fnOperationCallback(mqtt_client, MQTT_CLIENT_ON_UNSUBSCRIBE_ACK, (void*)&unsuback, mqtt_client->ctx);
                    break;
                }
                case PINGRESP_TYPE:
                    mqtt_client->timeSincePing = 0;
#ifndef NO_LOGGING
                    if (is_trace_enabled(mqtt_client))
                    {
                        log_incoming_trace(mqtt_client, "PINGRESP");
                    }
#endif
                    // Forward ping response to operation callback
                    mqtt_client->fnOperationCallback(mqtt_client, MQTT_CLIENT_ON_PING_RESPONSE, NULL, mqtt_client->ctx);
                    break;
                default:
                    break;
            }
        }
    }
    else
    {
        LogError("recv_complete_callback context failed.");
    }
}

//#if defined(__GNUC__)
//__attribute__ ((format (printf, 1, 2)))
//#endif
static void trace_logger(void* context, const char* format, ...)
{
    MQTT_CLIENT* mqtt_client = (MQTT_CLIENT*)context;
    if (mqtt_client == NULL)
    {
        LogError("client context NULL");
    }
    else if (format != NULL)
    {
#ifdef STRINGS_C_SPRINTF_BUFFER_SIZE
        size_t maxBufSize = STRINGS_C_SPRINTF_BUFFER_SIZE;
        char buf[STRINGS_C_SPRINTF_BUFFER_SIZE];
#else
        size_t maxBufSize = 0;
        char* buf = NULL;
#endif
        va_list arg_list;
        int length;
        va_start(arg_list, format);

        length = vsnprintf(buf, maxBufSize, format, arg_list);
        va_end(arg_list);
        if (length > 0)
        {
            size_t curr_pos;
            if (mqtt_client->trace_line == NULL)
            {
                mqtt_client->trace_alloc = length > 64 ? length+64 : 64;
                mqtt_client->trace_line = (char*)malloc(mqtt_client->trace_alloc);
                curr_pos = 0;
            }
            else
            {
                curr_pos = strlen(mqtt_client->trace_line);
                if (mqtt_client->trace_alloc <= curr_pos+length+1)
                {
                    mqtt_client->trace_alloc += length+64;
                    // Realloc
                    char* temp_alloc = realloc(mqtt_client->trace_line, mqtt_client->trace_alloc);
                    if (temp_alloc == NULL)
                    {
                        free(mqtt_client->trace_line);
                        mqtt_client->trace_line = NULL;
                        LogError("Failure allocating trace line");
                    }
                    else
                    {
                        mqtt_client->trace_line = temp_alloc;
                    }
                }
            }

            if (mqtt_client->trace_line != NULL)
            {
                va_start(arg_list, format);
                if (vsnprintf(mqtt_client->trace_line+curr_pos, length+1, format, arg_list) < 0)
                {
                    /* Codes_SRS_STRING_07_040: [If any error is encountered STRING_construct_sprintf shall return NULL.] */
                    free(mqtt_client->trace_line);
                    mqtt_client->trace_line = NULL;
                    LogError("Failure: vsnprintf formatting failed");
                }
                va_end(arg_list);
            }
        }
    }
}

static int init_codec_provider_info(MQTT_CLIENT* mqtt_client)
{
    int result;
    if ((mqtt_client->codec_handle = codec_v3_create(recv_complete_callback, mqtt_client)) == NULL)
    {
        LogError("Failure creating codec handle");
        result = MU_FAILURE;
    }
    else
    {
        result = 0;
    }
    return result;
}

MQTT_CLIENT_HANDLE mqtt_client_init(ON_MQTT_MESSAGE_RECV_CALLBACK msgRecv, ON_MQTT_OPERATION_CALLBACK operation_cb, void* opCallbackCtx, ON_MQTT_ERROR_CALLBACK onErrorCallBack, void* errorCBCtx)
{
    MQTT_CLIENT* result;
    /*Codes_SRS_MQTT_CLIENT_07_001: [If the parameters ON_MQTT_MESSAGE_RECV_CALLBACK is NULL then mqttclient_init shall return NULL.]*/
    if (msgRecv == NULL || operation_cb == NULL)
    {
        LogError("Invalid parameter specified msgRecv: %p, operation_cb: %p", msgRecv, operation_cb);
        result = NULL;
    }
    else
    {
        result = malloc(sizeof(MQTT_CLIENT));
        if (result == NULL)
        {
            /*Codes_SRS_MQTT_CLIENT_07_002: [If any failure is encountered then mqttclient_init shall return NULL.]*/
            LogError("mqtt_client_init failure: Allocation Failure");
        }
        else
        {
            memset(result, 0, sizeof(MQTT_CLIENT));
            /*Codes_SRS_MQTT_CLIENT_07_003: [mqttclient_init shall allocate MQTTCLIENT_DATA_INSTANCE and return the MQTTCLIENT_HANDLE on success.]*/
            result->packetState = UNKNOWN_TYPE;
            result->fnOperationCallback = operation_cb;
            result->ctx = opCallbackCtx;
            result->fnMessageRecv = msgRecv;
            result->fnOnErrorCallBack = onErrorCallBack;
            result->errorCBCtx = errorCBCtx;
            result->maxPingRespTime = DEFAULT_MAX_PING_RESPONSE_TIME;
            if ((result->packetTickCntr = tickcounter_create()) == NULL)
            {
                LogError("mqtt_client_init failure: tickcounter_create failure");
                free(result);
                result = NULL;
            }
            else if (init_codec_provider_info(result) != 0)
            {
                /*Codes_SRS_MQTT_CLIENT_07_002: [If any failure is encountered then mqttclient_init shall return NULL.]*/
                LogError("mqtt_client_init failure: mqtt_codec_create failure");
                tickcounter_destroy(result->packetTickCntr);
                free(result);
                result = NULL;
            }
        }
    }
    return result;
}

void mqtt_client_deinit(MQTT_CLIENT_HANDLE handle)
{
    if (handle != NULL)
    {
        MQTT_CLIENT* mqtt_client = (MQTT_CLIENT*)handle;
        tickcounter_destroy(mqtt_client->packetTickCntr);
        codec_v3_destroy(mqtt_client->codec_handle);
        clear_mqtt_options(mqtt_client);
        free(mqtt_client);
    }
}

int mqtt_client_connect(MQTT_CLIENT_HANDLE handle, XIO_HANDLE xioHandle, MQTT_CLIENT_OPTIONS* mqttOptions)
{
    int result;
    if (handle == NULL || mqttOptions == NULL || xioHandle == NULL)
    {
        LogError("mqtt_client_connect: NULL argument (handle = %p, mqttOptions = %p, xioHandle: %p)", handle, mqttOptions, xioHandle);
        result = MU_FAILURE;
    }
    else
    {
        MQTT_CLIENT* mqtt_client = (MQTT_CLIENT*)handle;
        mqtt_client->xioHandle = xioHandle;
        mqtt_client->packetState = UNKNOWN_TYPE;
        mqtt_client->qosValue = mqttOptions->qualityOfServiceValue;
        mqtt_client->keepAliveInterval = mqttOptions->keepAliveInterval;
        mqtt_client->maxPingRespTime = (DEFAULT_MAX_PING_RESPONSE_TIME < mqttOptions->keepAliveInterval/2) ? DEFAULT_MAX_PING_RESPONSE_TIME : mqttOptions->keepAliveInterval/2;

        if (clone_mqtt_options(mqtt_client, mqttOptions) != 0)
        {
            LogError("Error: Clone Mqtt Options failed");
            result = MU_FAILURE;
        }
        else if (xio_open(xioHandle, on_open_complete, mqtt_client, codec_v3_get_recv_func(), mqtt_client->codec_handle, on_io_error, mqtt_client) != 0)
        {
            LogError("Error: io_open failed");
            result = MU_FAILURE;
            mqtt_client->xioHandle = NULL;
            // Remove cloned options
            clear_mqtt_options(mqtt_client);
        }
        else
        {
            result = 0;
        }
    }
    return result;
}

int mqtt_client_publish(MQTT_CLIENT_HANDLE handle, MQTT_MESSAGE_HANDLE msgHandle)
{
    int result;
    MQTT_CLIENT* mqtt_client = (MQTT_CLIENT*)handle;
    if (mqtt_client == NULL || msgHandle == NULL)
    {
        LogError("Invalid parameter specified mqtt_client: %p, msgHandle: %p", mqtt_client, msgHandle);
        result = MU_FAILURE;
    }
    else
    {
        const APP_PAYLOAD* payload = mqttmessage_getApplicationMsg(msgHandle);
        if (payload == NULL)
        {
            LogError("Error: mqttmessage_getApplicationMsg failed");
            result = MU_FAILURE;
        }
        else
        {
            QOS_VALUE qos = mqttmessage_getQosType(msgHandle);
            bool isDuplicate = mqttmessage_getIsDuplicateMsg(msgHandle);
            bool isRetained = mqttmessage_getIsRetained(msgHandle);
            uint16_t packetId = mqttmessage_getPacketId(msgHandle);
            const char* topicName = mqttmessage_getTopicName(msgHandle);

            BUFFER_HANDLE publishPacket = codec_v3_publish(mqtt_client->codec_handle, qos, isDuplicate, isRetained, packetId, topicName, payload->message, payload->length);
            if (publishPacket == NULL)
            {
                LogError("Error: mqtt_codec_publish failed");
                result = MU_FAILURE;
            }
            else
            {
                mqtt_client->packetState = PUBLISH_TYPE;

                size_t size = BUFFER_length(publishPacket);
                if (send_packet_msg(mqtt_client, BUFFER_u_char(publishPacket), size) != 0)
                {
                    LogError("Error: mqtt_client_publish send failed");
                    result = MU_FAILURE;
                }
                else
                {
                    flush_trace_log(mqtt_client);
                    result = 0;
                }
                BUFFER_delete(publishPacket);
            }
        }
    }
    return result;
}

int mqtt_client_subscribe(MQTT_CLIENT_HANDLE handle, uint16_t packetId, SUBSCRIBE_PAYLOAD* subscribeList, size_t count)
{
    int result;
    MQTT_CLIENT* mqtt_client = (MQTT_CLIENT*)handle;
    if (mqtt_client == NULL || subscribeList == NULL || count == 0 || packetId == 0)
    {
        /*Codes_SRS_MQTT_CLIENT_07_013: [If any of the parameters handle, subscribeList is NULL or count is 0 then mqtt_client_subscribe shall return a non-zero value.]*/
        LogError("Invalid parameter specified mqtt_client: %p, subscribeList: %p, count: %lu, packetId: %d", mqtt_client, subscribeList, (unsigned long)count, packetId);
        result = MU_FAILURE;
    }
    else
    {
        BUFFER_HANDLE subPacket = codec_v3_subscribe(mqtt_client->codec_handle, packetId, subscribeList, count);
        if (subPacket == NULL)
        {
            /*Codes_SRS_MQTT_CLIENT_07_014: [If any failure is encountered then mqtt_client_subscribe shall return a non-zero value.]*/
            LogError("Error: mqtt_codec_subscribe failed");
            result = MU_FAILURE;
        }
        else
        {
            mqtt_client->packetState = SUBSCRIBE_TYPE;

            size_t size = BUFFER_length(subPacket);
            /*Codes_SRS_MQTT_CLIENT_07_015: [On success mqtt_client_subscribe shall send the MQTT SUBCRIBE packet to the endpoint.]*/
            if (send_packet_msg(mqtt_client, BUFFER_u_char(subPacket), size) != 0)
            {
                /*Codes_SRS_MQTT_CLIENT_07_014: [If any failure is encountered then mqtt_client_subscribe shall return a non-zero value.]*/
                LogError("Error: mqtt_client_subscribe send failed");
                result = MU_FAILURE;
            }
            else
            {
                flush_trace_log(mqtt_client);
                result = 0;
            }
            BUFFER_delete(subPacket);
        }
    }
    return result;
}

int mqtt_client_unsubscribe(MQTT_CLIENT_HANDLE handle, uint16_t packetId, const char** unsubscribeList, size_t count)
{
    int result;
    MQTT_CLIENT* mqtt_client = (MQTT_CLIENT*)handle;
    if (mqtt_client == NULL || unsubscribeList == NULL || count == 0 || packetId == 0)
    {
        /*Codes_SRS_MQTT_CLIENT_07_016: [If any of the parameters handle, unsubscribeList is NULL or count is 0 then mqtt_client_unsubscribe shall return a non-zero value.]*/
        LogError("Invalid parameter specified mqtt_client: %p, unsubscribeList: %p, count: %lu, packetId: %d", mqtt_client, unsubscribeList, (unsigned long)count, packetId);
        result = MU_FAILURE;
    }
    else
    {
        STRING_HANDLE trace_log = construct_trace_log_handle(mqtt_client);

        BUFFER_HANDLE unsubPacket = codec_v3_unsubscribe(mqtt_client->codec_handle, packetId, unsubscribeList, count);
        if (unsubPacket == NULL)
        {
            /*Codes_SRS_MQTT_CLIENT_07_017: [If any failure is encountered then mqtt_client_unsubscribe shall return a non-zero value.]*/
            LogError("Error: mqtt_codec_unsubscribe failed");
            result = MU_FAILURE;
        }
        else
        {
            mqtt_client->packetState = UNSUBSCRIBE_TYPE;

            size_t size = BUFFER_length(unsubPacket);
            /*Codes_SRS_MQTT_CLIENT_07_018: [On success mqtt_client_unsubscribe shall send the MQTT SUBCRIBE packet to the endpoint.]*/
            if (send_packet_msg(mqtt_client, BUFFER_u_char(unsubPacket), size) != 0)
            {
                /*Codes_SRS_MQTT_CLIENT_07_017: [If any failure is encountered then mqtt_client_unsubscribe shall return a non-zero value.].]*/
                LogError("Error: mqtt_client_unsubscribe send failed");
                result = MU_FAILURE;
            }
            else
            {
                log_outgoing_trace(mqtt_client, STRING_c_str(trace_log));
                result = 0;
            }
            BUFFER_delete(unsubPacket);
        }
        if (trace_log != NULL)
        {
            STRING_delete(trace_log);
        }
    }
    return result;
}

int mqtt_client_disconnect(MQTT_CLIENT_HANDLE handle, ON_MQTT_DISCONNECTED_CALLBACK callback, void* ctx)
{
    int result;
    MQTT_CLIENT* mqtt_client = (MQTT_CLIENT*)handle;
    if (mqtt_client == NULL)
    {
        /*Codes_SRS_MQTT_CLIENT_07_010: [If the parameters handle is NULL then mqtt_client_disconnect shall return a non-zero value.]*/
        result = MU_FAILURE;
    }
    else
    {
        if (mqtt_client->mqtt_status & MQTT_STATUS_CLIENT_CONNECTED)
        {
            BUFFER_HANDLE disconnectPacket = codec_v3_disconnect(mqtt_client->codec_handle);
            if (disconnectPacket == NULL)
            {
                /*Codes_SRS_MQTT_CLIENT_07_011: [If any failure is encountered then mqtt_client_disconnect shall return a non-zero value.]*/
                LogError("Error: mqtt_client_disconnect failed");
                mqtt_client->packetState = PACKET_TYPE_ERROR;
                result = MU_FAILURE;
            }
            else
            {
                /* Codes_SRS_MQTT_CLIENT_07_037: [ if callback is not NULL callback shall be called once the mqtt connection has been disconnected ] */
                mqtt_client->disconnect_cb = callback;
                mqtt_client->disconnect_ctx = ctx;
                mqtt_client->packetState = DISCONNECT_TYPE;

                size_t size = BUFFER_length(disconnectPacket);
                /*Codes_SRS_MQTT_CLIENT_07_012: [On success mqtt_client_disconnect shall send the MQTT DISCONNECT packet to the endpoint.]*/
                if (send_packet_msg(mqtt_client, BUFFER_u_char(disconnectPacket), size) != 0)
                {
                    /*Codes_SRS_MQTT_CLIENT_07_011: [If any failure is encountered then mqtt_client_disconnect shall return a non-zero value.]*/
                    LogError("Error: mqtt_client_disconnect send failed");
                    result = MU_FAILURE;
                }
                else
                {
                    if (is_trace_enabled(mqtt_client))
                    {
                        log_outgoing_trace(mqtt_client, "DISCONNECT");
                    }
                    result = 0;
                }
                BUFFER_delete(disconnectPacket);
            }
            clear_mqtt_options(mqtt_client);
        }
        else
        {
            // If the client is not connected then just close the underlying socket
            mqtt_client->disconnect_cb = callback;
            mqtt_client->disconnect_ctx = ctx;

            close_connection(mqtt_client);
            clear_mqtt_options(mqtt_client);
            result = 0;
        }
    }
    return result;
}

void mqtt_client_dowork(MQTT_CLIENT_HANDLE handle)
{
    MQTT_CLIENT* mqtt_client = (MQTT_CLIENT*)handle;
    /*Codes_SRS_MQTT_CLIENT_18_001: [If the client is disconnected, mqtt_client_dowork shall do nothing.]*/
    /*Codes_SRS_MQTT_CLIENT_07_023: [If the parameter handle is NULL then mqtt_client_dowork shall do nothing.]*/
    if (mqtt_client != NULL && mqtt_client->xioHandle != NULL)
    {
        if (mqtt_client->mqtt_status & MQTT_STATUS_PENDING_CLOSE)
        {
            close_connection(mqtt_client);
            // turn off pending close
            mqtt_client->mqtt_status &= ~MQTT_STATUS_PENDING_CLOSE;
        }
        else
        {
            /*Codes_SRS_MQTT_CLIENT_07_024: [mqtt_client_dowork shall call the xio_dowork function to complete operations.]*/
            xio_dowork(mqtt_client->xioHandle);

            /*Codes_SRS_MQTT_CLIENT_07_025: [mqtt_client_dowork shall retrieve the the last packet send value and ...]*/
            if (mqtt_client->mqtt_status & MQTT_STATUS_SOCKET_CONNECTED &&
                mqtt_client->mqtt_status & MQTT_STATUS_CLIENT_CONNECTED &&
                mqtt_client->keepAliveInterval > 0)
            {
                tickcounter_ms_t current_ms;
                if (tickcounter_get_current_ms(mqtt_client->packetTickCntr, &current_ms) != 0)
                {
                    LogError("Error: tickcounter_get_current_ms failed");
                }
                else
                {
                    /* Codes_SRS_MQTT_CLIENT_07_035: [If the timeSincePing has expired past the maxPingRespTime then mqtt_client_dowork shall call the Error Callback function with the message MQTT_CLIENT_NO_PING_RESPONSE] */
                    if (mqtt_client->timeSincePing > 0 && ((current_ms - mqtt_client->timeSincePing)/1000) > mqtt_client->maxPingRespTime)
                    {
                        // We haven't gotten a ping response in the alloted time
                        set_error_callback(mqtt_client, MQTT_CLIENT_NO_PING_RESPONSE);
                        mqtt_client->timeSincePing = 0;
                        mqtt_client->packetSendTimeMs = 0;
                        mqtt_client->packetState = UNKNOWN_TYPE;
                    }
                    else if (((current_ms - mqtt_client->packetSendTimeMs) / 1000) >= mqtt_client->keepAliveInterval)
                    {
                        /*Codes_SRS_MQTT_CLIENT_07_026: [if keepAliveInternal is > 0 and the send time is greater than the MQTT KeepAliveInterval then it shall construct an MQTT PINGREQ packet.]*/
                        BUFFER_HANDLE pingPacket = codec_v3_ping();
                        if (pingPacket != NULL)
                        {
                            size_t size = BUFFER_length(pingPacket);
                            (void)send_packet_msg(mqtt_client, BUFFER_u_char(pingPacket), size);
                            BUFFER_delete(pingPacket);
                            (void)tickcounter_get_current_ms(mqtt_client->packetTickCntr, &mqtt_client->timeSincePing);

                            if (is_trace_enabled(mqtt_client))
                            {
                                log_outgoing_trace(mqtt_client, "PINGREQ");
                            }
                        }
                    }
                }
            }
        }
    }
}

void mqtt_client_set_trace(MQTT_CLIENT_HANDLE handle, bool traceOn, bool rawBytesOn)
{
    AZURE_UNREFERENCED_PARAMETER(handle);
    AZURE_UNREFERENCED_PARAMETER(traceOn);
    AZURE_UNREFERENCED_PARAMETER(rawBytesOn);
#ifndef NO_LOGGING
    if (handle != NULL)
    {
        if (traceOn)
        {
            handle->mqtt_flags |= MQTT_FLAGS_LOG_TRACE;
        }
        else
        {
            handle->mqtt_flags &= ~MQTT_FLAGS_LOG_TRACE;
        }
#ifdef ENABLE_RAW_TRACE
        if (rawBytesOn)
        {
            handle->mqtt_flags |= MQTT_FLAGS_RAW_TRACE;
        }
        else
        {
            handle->mqtt_flags &= ~MQTT_FLAGS_RAW_TRACE;
        }
#endif
        codec_v3_set_trace(handle->codec_handle, trace_logger, handle);
    }
#endif
}
