// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

#ifndef MQTT_CODEC_UTIL_H
#define MQTT_CODEC_UTIL_H

#ifdef __cplusplus
#include <cstdint>
#include <cstddef>
extern "C" {
#else
#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>
#endif // __cplusplus

#include "umock_c/umock_c_prod.h"

#include "azure_c_shared_utility/buffer_.h"
#include "azure_umqtt_c/mqttconst.h"


#define NEXT_128_CHUNK                      0x80

typedef void(*ON_PACKET_COMPLETE_CALLBACK)(void* context, CONTROL_PACKET_TYPE packet, int flags, BUFFER_HANDLE headerData);
typedef void(*TRACE_LOG_CALLBACK)(void* context, const char* log_value, ...);

MOCKABLE_FUNCTION(, int, construct_fixed_header, BUFFER_HANDLE, ctrl_packet, CONTROL_PACKET_TYPE, packet_type, uint8_t, flags);
MOCKABLE_FUNCTION(, BUFFER_HANDLE, construct_connect_var_header, TRACE_LOG_CALLBACK, trace_func, void*, trace_ctx, const MQTT_CLIENT_OPTIONS*, mqtt_options, uint8_t, protocol_level);

extern int encode_variable_byte_integer(uint8_t stream_bytes[4], size_t* pos, uint32_t* vbi_value);

MOCKABLE_FUNCTION(, const char*, retrieve_qos_value, QOS_VALUE, value);
MOCKABLE_FUNCTION(, void, byteutil_writeByte, uint8_t**, buffer, uint8_t, value);
MOCKABLE_FUNCTION(, void, byteutil_writeInt, uint8_t**, buffer, uint16_t, value);
MOCKABLE_FUNCTION(, void, byteutil_writeUTF, uint8_t**, buffer, const char*, stringData, uint16_t, len);
MOCKABLE_FUNCTION(, CONTROL_PACKET_TYPE, processControlPacketType, uint8_t, pktByte, int*, flags);

#ifdef __cplusplus
}
#endif // __cplusplus

#endif // MQTT_CODEC_UTIL_H