// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

#include <stdlib.h>
#include "azure_c_shared_utility/gballoc.h"
#include "azure_c_shared_utility/xlogging.h"
#include "azure_c_shared_utility/buffer_.h"

#include "azure_umqtt_c/mqtt_properties.h"

typedef struct MQTT_PROP_ITEM_TAG
{
    MQTT_PROPERTY_TYPE type;
    union
    {
        uint8_t byte_value;
        uint16_t two_byte_value;
        uint32_t four_byte_value;
        char* string_value;
        BUFFER_HANDLE binary_value;

    } value_item;
} MQTT_PROP_ITEM;

typedef struct MQTT_PROPERTY_TAG
{
    size_t cnt;
} MQTT_PROPERTY;

typedef struct PROPERTY_ITERATOR_TAG
{
    size_t index;
    MQTT_PROP_ITEM* curr_item;
} PROPERTY_ITERATOR;

static MQTT_PROP_VALUE_TYPE get_prop_value_type(MQTT_PROPERTY_TYPE prop_type)
{
    MQTT_PROP_VALUE_TYPE result;
    switch (prop_type)
    {
        case PAYLOAD_FORMAT_INDICATOR:
        case MSG_EXPIRY_INTERVAL:
        case CONTENT_TYPE:
        case RESPONSE_TOPIC:
        case CORRELATION_DATA:
        case SUBSCRIPTION_ID:
        case SESSION_EXPIRY_INTERVAL:
        case ASSIGNED_CLIENT_ID:
        case SERVER_KEEP_ALIVE:
        case AUTHENTICATION_METHOD:
        case AUTHENTICATION_DATA:
        case REQUEST_PROBLEM_INFO:
        case WILL_DELAY_INTERVAL:
        case REQUEST_RESPONSE_INFO:
        case RESPONSE_INFO:
        case SERVER_REFERENCE:
        case REASON_STRING:
        case RECEIVE_MAXIMUM:
        case TOPIC_ALIAS_MAXIMUM:
        case TOPIC_ALIAS:
        case MAXIMUM_QOS:
        case RETAIN_AVAILABLE:
        case USER_PROPERTY:
        case MAXIMUM_PACKET_SIZE:
        case WILDCARD_SUB_AVAILABLE:
        case SUBSCRIPTION_ID_AVAILABLE:
        case SHARED_SUB_AVAILABLE:
            break;
        default:
            result = VALUE_TYPE_UNKNOWN;
            break;
    }
    return result;
}

MQTT_PROPERTY_HANDLE mqtt_prop_create(void)
{
    MQTT_PROPERTY* result;
    if ((result = malloc(sizeof(MQTT_PROPERTY))) == NULL)
    {
        LogError("failure allocating property object");
    }
    else
    {

    }
    return result;
}

void mqtt_prop_destroy(MQTT_PROPERTY_HANDLE handle)
{
    if (handle != NULL)
    {
        free(handle);
    }
}

int mqtt_prop_add_byte_property(MQTT_PROPERTY_TYPE type, uint8_t value)
{
    int result;
    MQTT_PROP_ITEM* prop_item;
    if ((prop_item = (MQTT_PROP_ITEM*)malloc(sizeof(MQTT_PROP_ITEM))) == NULL)
    {
        LogError("failure allocating property item");
        result = MU_FAILURE;
    }
    else
    {
        prop_item->type = type;
        prop_item->value_item.byte_value = value;
        result = 0;
    }
    return result;
}
