// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

#ifndef MQTT_PROPERTIES_H
#define MQTT_PROPERTIES_H

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

#define MQTT_PROPERTY_TYPE_VALUES       \
    PAYLOAD_FORMAT_INDICATOR = 0x01,    \
    MSG_EXPIRY_INTERVAL = 0x02,         \
    CONTENT_TYPE = 0x03,                \
    RESPONSE_TOPIC = 0x08,              \
    CORRELATION_DATA = 0x09,            \
    SUBSCRIPTION_ID = 0x0B,             \
    SESSION_EXPIRY_INTERVAL = 0x11,     \
    ASSIGNED_CLIENT_ID = 0x12,          \
    SERVER_KEEP_ALIVE = 0x13,           \
    AUTHENTICATION_METHOD = 0x15,       \
    AUTHENTICATION_DATA = 0x16,         \
    REQUEST_PROBLEM_INFO = 0X17,        \
    WILL_DELAY_INTERVAL = 0X18,         \
    REQUEST_RESPONSE_INFO = 0X19,       \
    RESPONSE_INFO = 0X1A,               \
    SERVER_REFERENCE = 0X1C,            \
    REASON_STRING = 0X1F,               \
    RECEIVE_MAXIMUM = 0X21,             \
    TOPIC_ALIAS_MAXIMUM = 0X22,         \
    TOPIC_ALIAS = 0X23,                 \
    MAXIMUM_QOS = 0X24,                 \
    RETAIN_AVAILABLE = 0X25,            \
    USER_PROPERTY = 0X26,               \
    MAXIMUM_PACKET_SIZE = 0X27,         \
    WILDCARD_SUB_AVAILABLE = 0X28,      \
    SUBSCRIPTION_ID_AVAILABLE = 0X29,   \
    SHARED_SUB_AVAILABLE = 0x2A

MU_DEFINE_ENUM(MQTT_PROPERTY_TYPE, MQTT_PROPERTY_TYPE_VALUES);

#define MQTT_PROP_VALUE_TYPE_VALUES     \
    VALUE_TYPE_UNKNOWN,                 \
    VALUE_TYPE_BYTE,                    \
    VALUE_TYPE_2_BYTE_INT,              \
    VALUE_TYPE_4_BYTE_INT,              \
    VALUE_TYPE_UTF_8_STRING,            \
    VALUE_TYPE_BINARY,                  \
    VALUE_TYPE_VARIABLE_BYTE_INT,       \
    VALUE_TYPE_STRING_PAIR

MU_DEFINE_ENUM(MQTT_PROP_VALUE_TYPE, MQTT_PROP_VALUE_TYPE_VALUES);

typedef struct MQTT_PROPERTY_TAG* MQTT_PROPERTY_HANDLE;
typedef struct PROPERTY_ITERATOR_TAG* PROPERTY_ITERATOR_HANDLE;

MOCKABLE_FUNCTION(, MQTT_PROPERTY_HANDLE, mqtt_prop_create);
MOCKABLE_FUNCTION(, void, mqtt_prop_destroy, MQTT_PROPERTY_HANDLE, handle);

MOCKABLE_FUNCTION(, int, mqtt_prop_add_byte_property, MQTT_PROPERTY_TYPE, type, uint8_t, value);
MOCKABLE_FUNCTION(, int, mqtt_prop_add_2_byte_property, MQTT_PROPERTY_TYPE, type, uint16_t, value);
MOCKABLE_FUNCTION(, int, mqtt_prop_add_4_byte_property, MQTT_PROPERTY_TYPE, type, uint32_t, value);
MOCKABLE_FUNCTION(, int, mqtt_prop_add_binary_property, MQTT_PROPERTY_TYPE, type, const unsigned char*, value, size_t, length);
MOCKABLE_FUNCTION(, int, mqtt_prop_add_string_property, MQTT_PROPERTY_TYPE, type, const char*, value);
MOCKABLE_FUNCTION(, int, mqtt_prop_add_string_array_property, MQTT_PROPERTY_TYPE, type, const char**, value, size_t, count);

MOCKABLE_FUNCTION(, PROPERTY_ITERATOR_HANDLE, mqtt_prop_get_iterator);
MOCKABLE_FUNCTION(, MQTT_PROPERTY_TYPE, mqtt_prop_get_property_type, PROPERTY_ITERATOR_HANDLE, handle);
MOCKABLE_FUNCTION(, const void*, mqtt_prop_get_next_value, PROPERTY_ITERATOR_HANDLE, handle);


#ifdef __cplusplus
}
#endif // __cplusplus

#endif // MQTT_PROPERTIES_H