// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

#ifndef MQTT_CLIENT_H
#define MQTT_CLIENT_H

#include "azure_c_shared_utility/xio.h"
#include "azure_umqtt_c/mqttconst.h"
#include "azure_umqtt_c/mqtt_message.h"
#include "umock_c/umock_c_prod.h"
#include "azure_macro_utils/macro_utils.h"

#ifdef __cplusplus
#include <cstdint>
extern "C" {
#else
#include <stdint.h>
#endif // __cplusplus

typedef struct MQTT_CLIENT_V5_TAG* MQTT_CLIENT_V5_HANDLE;

#define MQTT_V5_CLIENT_EVENT_VALUES     \
    MQTT_V5_CLIENT_ON_CONNACK,          \
    MQTT_V5_CLIENT_ON_PUBLISH_ACK,      \
    MQTT_V5_CLIENT_ON_PUBLISH_RECV,     \
    MQTT_V5_CLIENT_ON_PUBLISH_REL,      \
    MQTT_V5_CLIENT_ON_PUBLISH_COMP,     \
    MQTT_V5_CLIENT_ON_SUBSCRIBE_ACK,    \
    MQTT_V5_CLIENT_ON_UNSUBSCRIBE_ACK,  \
    MQTT_V5_CLIENT_ON_PING_RESPONSE,    \
    MQTT_V5_CLIENT_ON_DISCONNECT,       \
    MQTT_V5_CLIENT_ON_AUTH

MU_DEFINE_ENUM(MQTT_V5_CLIENT_EVENT_RESULT, MQTT_V5_CLIENT_EVENT_VALUES);

#define MQTT_V5_CLIENT_EVENT_ERROR_VALUES     \
    MQTT_V5_CLIENT_CONNECTION_ERROR,          \
    MQTT_V5_CLIENT_PARSE_ERROR,               \
    MQTT_V5_CLIENT_MEMORY_ERROR,              \
    MQTT_V5_CLIENT_COMMUNICATION_ERROR,       \
    MQTT_V5_CLIENT_NO_PING_RESPONSE,          \
    MQTT_V5_CLIENT_UNKNOWN_ERROR

MU_DEFINE_ENUM(MQTT_V5_CLIENT_EVENT_ERROR, MQTT_V5_CLIENT_EVENT_ERROR_VALUES);


#define MQTT_V5_REASON_CODE_VALUES                      \
    MQTT_V5_REASON_CODE_SUCCESS_NORMAL_GRANTED, 0x00,   \
    MQTT_V5_REASON_CODE_QOS1_GRANTED, 0x01              \

MU_DEFINE_ENUM_2_WITHOUT_INVALID(MQTT_V5_REASON_CODE, MQTT_V5_REASON_CODE_VALUES);

#define SESSION_NO_EXPIRY       0xFFFFFFFF

typedef void(*ON_MQTT_OPERATION_CALLBACK)(MQTT_CLIENT_V5_HANDLE handle, MQTT_V5_CLIENT_EVENT_RESULT action_result, const void* msg_info, void* user_ctx);
typedef void(*ON_MQTT_V5_ERROR_CALLBACK)(MQTT_V5_CLIENT_EVENT_ERROR error, void* user_ctx);
typedef void(*ON_MQTT_MESSAGE_RECV_CALLBACK)(MQTT_MESSAGE_HANDLE msg_handle, void* user_ctx);
typedef void(*ON_MQTT_DISCONNECTED_CALLBACK)(void* user_ctx);

MOCKABLE_FUNCTION(, MQTT_CLIENT_V5_HANDLE, mqtt_client_v5_create, ON_MQTT_MESSAGE_RECV_CALLBACK, msg_recv_callback, ON_MQTT_OPERATION_CALLBACK, operation_cb, void*, op_user_ctx, ON_MQTT_V5_ERROR_CALLBACK, on_error_cb, void*, errorCBCtx);
MOCKABLE_FUNCTION(, void, mqtt_client_v5_destroy, MQTT_CLIENT_V5_HANDLE, handle);

MOCKABLE_FUNCTION(, int, mqtt_client_v5_connect, MQTT_CLIENT_V5_HANDLE, handle, XIO_HANDLE, xioHandle, MQTT_CLIENT_OPTIONS*, mqttOptions);
MOCKABLE_FUNCTION(, int, mqtt_client_v5_disconnect, MQTT_CLIENT_V5_HANDLE, handle, ON_MQTT_DISCONNECTED_CALLBACK, callback, void*, ctx);

MOCKABLE_FUNCTION(, int, mqtt_client_v5_subscribe, MQTT_CLIENT_V5_HANDLE, handle, uint16_t, packetId, SUBSCRIBE_PAYLOAD*, subscribeList, size_t, count);
MOCKABLE_FUNCTION(, int, mqtt_client_v5_unsubscribe, MQTT_CLIENT_V5_HANDLE, handle, uint16_t, packetId, const char**, unsubscribeList, size_t, count);

MOCKABLE_FUNCTION(, int, mqtt_client_v5_publish, MQTT_CLIENT_V5_HANDLE, handle, MQTT_MESSAGE_HANDLE, msgHandle);

MOCKABLE_FUNCTION(, void, mqtt_client_v5_dowork, MQTT_CLIENT_V5_HANDLE, handle);

MOCKABLE_FUNCTION(, void, mqtt_client_v5_set_trace, MQTT_CLIENT_V5_HANDLE, handle, bool, traceOn, bool, rawBytesOn);

#ifdef __cplusplus
}
#endif // __cplusplus

#endif // MQTTCLIENT_H
