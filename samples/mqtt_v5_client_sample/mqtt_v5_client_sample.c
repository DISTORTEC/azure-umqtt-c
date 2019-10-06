// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
#include <stdlib.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdint.h>
#include "azure_umqtt_c/mqtt_client_v5.h"
#include "azure_c_shared_utility/socketio.h"
#include "azure_c_shared_utility/platform.h"

static const char* TOPIC_NAME_A = "msgA";
static const char* TOPIC_NAME_B = "msgB";
static const char* APP_NAME_A = "This is the app msg A.";

static uint16_t PACKET_ID_VALUE = 11;
static bool g_continue = true;
static bool g_close_complete = true;

#define PORT_NUM_UNENCRYPTED        1883
#define PORT_NUM_ENCRYPTED          8883
#define PORT_NUM_ENCRYPTED_CERT     8884

#define DEFAULT_MSG_TO_SEND         1

static const char* QosToString(QOS_VALUE qosValue)
{
    switch (qosValue)
    {
        case DELIVER_AT_LEAST_ONCE: return "Deliver_At_Least_Once";
        case DELIVER_EXACTLY_ONCE: return "Deliver_Exactly_Once";
        case DELIVER_AT_MOST_ONCE: return "Deliver_At_Most_Once";
        case DELIVER_FAILURE: return "Deliver_Failure";
    }
    return "";
}

static void OnRecvCallback(MQTT_MESSAGE_HANDLE msgHandle, void* context)
{
    (void)context;
    const APP_PAYLOAD* appMsg = mqttmessage_getApplicationMsg(msgHandle);

    (void)printf("Incoming Msg: Packet Id: %d\r\nQOS: %s\r\nTopic Name: %s\r\nIs Retained: %s\r\nIs Duplicate: %s\r\nApp Msg: ", mqttmessage_getPacketId(msgHandle),
        QosToString(mqttmessage_getQosType(msgHandle) ),
        mqttmessage_getTopicName(msgHandle),
        mqttmessage_getIsRetained(msgHandle) ? "true" : "fale",
        mqttmessage_getIsDuplicateMsg(msgHandle) ? "true" : "fale"
        );
    for (size_t index = 0; index < appMsg->length; index++)
    {
        (void)printf("0x%x", appMsg->message[index]);
    }

    (void)printf("\r\n");
}

static void OnCloseComplete(void* context)
{
    (void)context;

    (void)printf("On Close Connection called\r\n");
    g_close_complete = false;
}

static void OnOperationComplete(MQTT_CLIENT_V5_HANDLE handle, MQTT_V5_CLIENT_EVENT_RESULT action_result, const void* msgInfo, void* callbackCtx)
{
    (void)msgInfo;
    (void)callbackCtx;
    switch (action_result)
    {
/*        case MQTT_CLIENT_ON_CONNACK:
        {
            (void)printf("ConnAck function called\r\n");

            SUBSCRIBE_PAYLOAD subscribe[2];
            subscribe[0].subscribeTopic = TOPIC_NAME_A;
            subscribe[0].qosReturn = DELIVER_AT_MOST_ONCE;
            subscribe[1].subscribeTopic = TOPIC_NAME_B;
            subscribe[1].qosReturn = DELIVER_EXACTLY_ONCE;

            if (mqtt_client_v5_subscribe(handle, PACKET_ID_VALUE++, subscribe, sizeof(subscribe) / sizeof(subscribe[0])) != 0)
            {
                (void)printf("%d: mqtt_client_v5_subscribe failed\r\n", __LINE__);
                g_continue = false;
            }
            mqtt_client_v5_disconnect(handle, NULL, NULL);
            break;
        }
        case MQTT_CLIENT_ON_SUBSCRIBE_ACK:
        {
            MQTT_MESSAGE_HANDLE msg = mqttmessage_create(PACKET_ID_VALUE++, TOPIC_NAME_A, DELIVER_EXACTLY_ONCE, (const uint8_t*)APP_NAME_A, strlen(APP_NAME_A));
            if (msg == NULL)
            {
                (void)printf("%d: mqttmessage_create failed\r\n", __LINE__);
                g_continue = false;
            }
            else
            {
                if (mqtt_client_v5_publish(handle, msg))
                {
                    (void)printf("%d: mqtt_client_v5_publish failed\r\n", __LINE__);
                    g_continue = false;
                }
                mqttmessage_destroy(msg);
            }
            // Now send a message that will get
            break;
        }
        case MQTT_CLIENT_ON_PUBLISH_ACK:
        {
            break;
        }
        case MQTT_CLIENT_ON_PUBLISH_RECV:
        {
            break;
        }
        case MQTT_CLIENT_ON_PUBLISH_REL:
        {
            break;
        }
        case MQTT_CLIENT_ON_PUBLISH_COMP:
        {
            if (mqtt_client_v5_unsubscribe(handle, PACKET_ID_VALUE++, TOPIC_NAME_A, 1) != 0)
            {
                (void)printf("%d: mqtt_client_v5_unsubscribe failed\r\n", __LINE__);
                g_continue = false;
            }
            // Done so send disconnect
            break;
        }
        case MQTT_CLIENT_ON_DISCONNECT:
            g_continue = false;
            break;
        case MQTT_CLIENT_ON_UNSUBSCRIBE_ACK:
        {
            // Done so send disconnect
            mqtt_client_v5_disconnect(handle, NULL, NULL);
            break;
        }
        case MQTT_CLIENT_ON_PING_RESPONSE:
            break;*/
        default:
        {
            (void)printf("unexpected value received for enumeration (%d)\n", (int)action_result);
        }
    }
}

static void OnErrorComplete(MQTT_V5_CLIENT_EVENT_ERROR error, void* callbackCtx)
{
    (void)callbackCtx;
    switch (error)
    {
        case MQTT_V5_CLIENT_CONNECTION_ERROR:
        case MQTT_V5_CLIENT_PARSE_ERROR:
        case MQTT_V5_CLIENT_MEMORY_ERROR:
        case MQTT_V5_CLIENT_COMMUNICATION_ERROR:
        case MQTT_V5_CLIENT_NO_PING_RESPONSE:
        case MQTT_V5_CLIENT_UNKNOWN_ERROR:
            g_continue = false;
            break;
    }
}

int main(void)
{
    if (platform_init() != 0)
    {
        (void)printf("platform_init failed\r\n");
    }
    else
    {
        MQTT_CLIENT_V5_HANDLE mqttHandle = mqtt_client_v5_create(OnRecvCallback, OnOperationComplete, NULL, OnErrorComplete, NULL);
        if (mqttHandle == NULL)
        {
            (void)printf("mqtt_client_v5_init failed\r\n");
        }
        else
        {
            mqtt_client_v5_set_trace(mqttHandle, true, false);

            MQTT_CLIENT_OPTIONS options = { 0 };
            options.clientId = "azureiotclient";
            options.willMessage = NULL;
            options.username = NULL;
            options.password = NULL;
            options.keepAliveInterval = 10;
            options.useCleanSession = true;
            options.qualityOfServiceValue = DELIVER_AT_MOST_ONCE;

            SOCKETIO_CONFIG config = {"test.mosquitto.org", PORT_NUM_UNENCRYPTED, NULL};

            XIO_HANDLE xio = xio_create(socketio_get_interface_description(), &config);
            if (xio == NULL)
            {
                (void)printf("xio_create failed\r\n");
            }
            else
            {
                if (mqtt_client_v5_connect(mqttHandle, xio, &options) != 0)
                {
                    (void)printf("mqtt_client_v5_connect failed\r\n");
                }
                else
                {
                    do
                    {
                        mqtt_client_v5_dowork(mqttHandle);
                    } while (g_continue);
                }
                xio_close(xio, OnCloseComplete, NULL);

                // Wait for the close connection gets called
                do
                {
                    mqtt_client_v5_dowork(mqttHandle);
                } while (g_close_complete);
                xio_destroy(xio);
            }
            mqtt_client_v5_destroy(mqttHandle);
        }
        platform_deinit();
    }

    printf("Press Enter to close.");
    getchar();
    return 0;
}
