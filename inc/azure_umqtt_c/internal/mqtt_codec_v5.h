// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

#ifndef MQTT_CODEC_V5_H
#define MQTT_CODEC_V5_H

#include "azure_c_shared_utility/xio.h"
#include "azure_c_shared_utility/buffer_.h"
#include "umock_c/umock_c_prod.h"

#include "azure_umqtt_c/mqttconst.h"
#include "azure_umqtt_c/internal/mqtt_codec_util.h"
#include "azure_umqtt_c/mqtt_properties.h"

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus

typedef struct CODEC_V5_INSTANCE_TAG* MQTT_CODEC_V5_HANDLE;

MOCKABLE_FUNCTION(, MQTT_CODEC_V5_HANDLE, codec_v5_create, ON_PACKET_COMPLETE_CALLBACK, on_packet_complete_cb, void*, context);
MOCKABLE_FUNCTION(, void, codec_v5_destroy, MQTT_CODEC_V5_HANDLE, handle);
MOCKABLE_FUNCTION(, BUFFER_HANDLE, codec_v5_connect, MQTT_CODEC_V5_HANDLE, handle, const MQTT_CLIENT_OPTIONS*, mqttOptions, MQTT_PROPERTY_HANDLE, prop_handle);
MOCKABLE_FUNCTION(, BUFFER_HANDLE, codec_v5_disconnect, MQTT_CODEC_V5_HANDLE, handle, const DISCONNECT_INFO*, info);
MOCKABLE_FUNCTION(, BUFFER_HANDLE, codec_v5_publish, MQTT_CODEC_V5_HANDLE, handle, QOS_VALUE, qosValue, bool, duplicateMsg, bool, serverRetain, uint16_t, packetId, const char*, topicName, const uint8_t*, msgBuffer, size_t, buffLen);
MOCKABLE_FUNCTION(, BUFFER_HANDLE, codec_v5_publish_ack, uint16_t, packetId);
MOCKABLE_FUNCTION(, BUFFER_HANDLE, codec_v5_publish_received, uint16_t, packetId);
MOCKABLE_FUNCTION(, BUFFER_HANDLE, codec_v5_publish_release, uint16_t, packetId);
MOCKABLE_FUNCTION(, BUFFER_HANDLE, codec_v5_publish_complete, uint16_t, packetId);
MOCKABLE_FUNCTION(, BUFFER_HANDLE, codec_v5_ping);
MOCKABLE_FUNCTION(, BUFFER_HANDLE, codec_v5_subscribe, MQTT_CODEC_V5_HANDLE, handle, uint16_t, packetId, SUBSCRIBE_PAYLOAD*, subscribeList, size_t, count);
MOCKABLE_FUNCTION(, BUFFER_HANDLE, codec_v5_unsubscribe, MQTT_CODEC_V5_HANDLE, handle, uint16_t,  packetId, const char**, unsubscribeList, size_t, count);
MOCKABLE_FUNCTION(, int, codec_v5_set_trace, MQTT_CODEC_V5_HANDLE, handle, TRACE_LOG_CALLBACK, trace_func, void*, trace_ctx);
MOCKABLE_FUNCTION(, ON_BYTES_RECEIVED, codec_v5_get_recv_func);

#ifdef __cplusplus
}
#endif // __cplusplus

#endif // MQTT_CODEC_V5_H
