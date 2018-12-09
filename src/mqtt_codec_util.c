// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

#include <stdlib.h>
#include "azure_c_shared_utility/gballoc.h"
#include "azure_c_shared_utility/xlogging.h"

#include "azure_umqtt_c/mqtt_codec_util.h"

#include <inttypes.h>

#define PACKET_TYPE_BYTE(p)                 (CONTROL_PACKET_TYPE)((uint8_t)(((uint8_t)(p)) & 0xf0))
#define FLAG_VALUE_BYTE(p)                  ((uint8_t)(((uint8_t)(p)) & 0xf))
#define CONNECT_VAR_HEADER_SIZE             10

BUFFER_HANDLE construct_connect_var_header(TRACE_LOG_VALUE trace_func, void* trace_ctx, const MQTT_CLIENT_OPTIONS* mqtt_options, uint8_t protocol_level)
{
    BUFFER_HANDLE result;
    if ((result = BUFFER_create_with_size(CONNECT_VAR_HEADER_SIZE)) == NULL)
    {
        LogError("Failure creating buffer");
    }
    else
    {
        if (trace_func != NULL)
        {
            trace_func(trace_ctx, " | VER: %d | KEEPALIVE: %d", protocol_level, mqtt_options->keepAliveInterval);
        }

        uint8_t* iterator = BUFFER_u_char(result);
        byteutil_writeUTF(&iterator, "MQTT", 4);
        byteutil_writeByte(&iterator, protocol_level);
        byteutil_writeByte(&iterator, 0); // Flags will be entered later
        byteutil_writeInt(&iterator, mqtt_options->keepAliveInterval);
    }
    return result;
}

int construct_fixed_header(BUFFER_HANDLE ctrl_packet, CONTROL_PACKET_TYPE packet_type, uint8_t flags)
{
    int result;
    size_t packet_len = BUFFER_length(ctrl_packet);
    uint8_t remain_len[4] ={ 0 };
    size_t index = 0;

    // Calculate the length of packet
    do
    {
        uint8_t encode = packet_len % 128;
        packet_len /= 128;
        // if there are more data to encode, set the top bit of this byte
        if (packet_len > 0)
        {
            encode |= NEXT_128_CHUNK;
        }
        remain_len[index++] = encode;
    } while (packet_len > 0);

    BUFFER_HANDLE fixed_hdr = BUFFER_create_with_size(index + 1);
    if (fixed_hdr == NULL)
    {
        LogError(FAILURE_MSG_CREATE_BUFFER);
        result = __FAILURE__;
    }
    else
    {
        uint8_t* iterator = BUFFER_u_char(fixed_hdr);
        *iterator = (uint8_t)packet_type | flags;
        iterator++;
        (void)memcpy(iterator, remain_len, index);

        result = BUFFER_prepend(ctrl_packet, fixed_hdr);
        BUFFER_delete(fixed_hdr);
    }
    return result;
}

const char* retrieve_qos_value(QOS_VALUE value)
{
    switch (value)
    {
        case DELIVER_AT_MOST_ONCE:
            return "DELIVER_AT_MOST_ONCE";
        case DELIVER_AT_LEAST_ONCE:
            return "DELIVER_AT_LEAST_ONCE";
        case DELIVER_EXACTLY_ONCE:
        default:
            return "DELIVER_EXACTLY_ONCE";
    }
}

void byteutil_writeByte(uint8_t** buffer, uint8_t value)
{
    if (buffer != NULL)
    {
        **buffer = value;
        (*buffer)++;
    }
}

void byteutil_writeInt(uint8_t** buffer, uint16_t value)
{
    if (buffer != NULL)
    {
        **buffer = (char)(value / 256);
        (*buffer)++;
        **buffer = (char)(value % 256);
        (*buffer)++;
    }
}

void byteutil_writeUTF(uint8_t** buffer, const char* stringData, uint16_t len)
{
    if (buffer != NULL)
    {
        byteutil_writeInt(buffer, len);
        (void)memcpy(*buffer, stringData, len);
        *buffer += len;
    }
}

CONTROL_PACKET_TYPE processControlPacketType(uint8_t pktByte, int* flags)
{
    CONTROL_PACKET_TYPE result;
    result = PACKET_TYPE_BYTE(pktByte);
    if (flags != NULL)
    {
        *flags = FLAG_VALUE_BYTE(pktByte);
    }
    return result;
}
