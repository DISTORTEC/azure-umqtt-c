// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

#ifndef MQTTCONST_H
#define MQTTCONST_H

#include "azure_c_shared_utility/crt_abstractions.h"
#include "azure_macro_utils/macro_utils.h"

#ifdef __cplusplus
#include <cstddef>
#include <cstdint>
extern "C" {
#else
#include <stddef.h>
#include <stdint.h>
#endif /* __cplusplus */

#define CONTROL_PACKET_TYPE_VALUES \
    CONNECT_TYPE, 0x10, \
    CONNACK_TYPE, 0x20, \
    PUBLISH_TYPE, 0x30, \
    PUBACK_TYPE, 0x40, \
    PUBREC_TYPE, 0x50, \
    PUBREL_TYPE, 0x60, \
    PUBCOMP_TYPE, 0x70, \
    SUBSCRIBE_TYPE, 0x80, \
    SUBACK_TYPE, 0x90, \
    UNSUBSCRIBE_TYPE, 0xA0, \
    UNSUBACK_TYPE, 0xB0, \
    PINGREQ_TYPE, 0xC0, \
    PINGRESP_TYPE, 0xD0, \
    DISCONNECT_TYPE, 0xE0, \
    PACKET_TYPE_ERROR, 0xE1, /* 0xE1 was assigned because ENUM_2 needs it */ \
    UNKNOWN_TYPE, 0xE2 /* 0xE2 was assigned because ENUM_2 needs it */

MU_DEFINE_ENUM_2(CONTROL_PACKET_TYPE, CONTROL_PACKET_TYPE_VALUES)

#define QOS_VALUE_VALUES \
    DELIVER_AT_MOST_ONCE, 0x00, \
    DELIVER_AT_LEAST_ONCE, 0x01, \
    DELIVER_EXACTLY_ONCE, 0x02, \
    DELIVER_FAILURE, 0x80

MU_DEFINE_ENUM_2(QOS_VALUE, QOS_VALUE_VALUES)

#define CODEC_STATE_VALUES      \
    CODEC_STATE_FIXED_HEADER,   \
    CODEC_STATE_VAR_HEADER,     \
    CODEC_STATE_PAYLOAD

MU_DEFINE_ENUM(CODEC_STATE_RESULT, CODEC_STATE_VALUES);

static const char* const FAILURE_MSG_CREATE_BUFFER = "Failure creating BUFFER";
static const char* const TRUE_CONST = "true";
static const char* const FALSE_CONST = "false";
static const char* const BUFF_ALLOCATION_ERROR_MSG = "Failure allocating Buffer";

typedef struct APP_PAYLOAD_TAG
{
    uint8_t* message;
    size_t length;
} APP_PAYLOAD;

typedef struct MQTT_CLIENT_OPTIONS_TAG
{
    char* clientId;
    char* willTopic;
    char* willMessage;
    char* username;
    char* password;
    uint16_t keepAliveInterval;
    bool messageRetain;
    bool useCleanSession;
    QOS_VALUE qualityOfServiceValue;
    bool log_trace;
} MQTT_CLIENT_OPTIONS;

typedef enum CONNECT_RETURN_CODE_TAG
{
    CONNECTION_ACCEPTED = 0x00,
    CONN_REFUSED_UNACCEPTABLE_VERSION = 0x01,
    CONN_REFUSED_ID_REJECTED = 0x02,
    CONN_REFUSED_SERVER_UNAVAIL = 0x03,
    CONN_REFUSED_BAD_USERNAME_PASSWORD = 0x04,
    CONN_REFUSED_NOT_AUTHORIZED = 0x05,
    CONN_REFUSED_UNKNOWN
} CONNECT_RETURN_CODE;

typedef enum DISCONNECT_REASON_CODE_TAG
{
    NORMAL_DISCONNECT = 0x00,
    DISCONNECT_WITH_WILL_MSG = 0x04,
    UNSPECIFIED_ERROR = 0x80,
    MALFORMED_PACKET = 0x81,
    PROTOCOL_ERROR = 0x82,
    IMPLEMENTATION_ERROR = 0x83,
    NOT_AUTHORIZED = 0x87,
    SERVER_BUSY = 0x89,
    SERVER_SHUTTING_DOWN = 0x8B,
    KEEP_ALIVE_TIMEOUT = 0x8D,
    SESSION_TAKEN_OVER = 0x8E,
    TOPIC_FILTER_INCORRECT = 0x8F,
    TOPIC_NAME_INVALID = 0x90,
    MAX_RECV_EXCEEDED = 0x93,
    TOPIC_ALIAS_INVALID = 0x94,
    PACKET_TOO_LARGE = 0x95,
    MSG_RATE_TOO_HIGH = 0x96,
    QUOTA_EXCEEDED = 0x97,
    ADMIN_ACTION = 0x98,
    PAYLOAD_FORMAT_INVALID = 0x99,
    RETAIN_NOT_SUPPORTED = 0x9A,
    QOS_NOT_SUPPORTED = 0x9B,
    USE_OTHER_SERVER = 0x9C,
    SERVER_MOVED = 0x9D,
    SHARED_SUB_NOT_SUPPORTED = 0x9E,
    CONN_RATE_EXCEEDED = 0x9F,
    MAX_CONN_TIME_EXCEEDED = 0xA0,
    SUBSCRIPTION_ID_NOT_SUPPORTED = 0xA1,
    WILDCARD_SUBSCRIPTION_NOT_SUPPORTED = 0xA2
} DISCONNECT_REASON_CODE;

typedef struct CONNECT_ACK_TAG
{
    bool isSessionPresent;
    CONNECT_RETURN_CODE returnCode;
} CONNECT_ACK;

typedef struct SUBSCRIBE_PAYLOAD_TAG
{
    const char* subscribeTopic;
    QOS_VALUE qosReturn;
} SUBSCRIBE_PAYLOAD;

typedef struct SUBSCRIBE_ACK_TAG
{
    uint16_t packetId;
    QOS_VALUE* qosReturn;
    size_t qosCount;
} SUBSCRIBE_ACK;

typedef struct UNSUBSCRIBE_ACK_TAG
{
    uint16_t packetId;
} UNSUBSCRIBE_ACK;

typedef struct PUBLISH_ACK_TAG
{
    uint16_t packetId;
} PUBLISH_ACK;

typedef struct DISCONNECT_INFO_TAG
{
    DISCONNECT_REASON_CODE reason_code;
    uint8_t session_expiry;
    const char* reason;
    char* user_property; // Array
    const char* additional_server;
} DISCONNECT_INFO;

#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif // MQTTCONST_H
