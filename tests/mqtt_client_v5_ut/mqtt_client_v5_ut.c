// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.


#ifdef __cplusplus
#include <cstdlib>
#include <cstddef>
#include <cstdint>
#else
#include <stdlib.h>
#include <stddef.h>
#include <stdbool.h>
#include <stdint.h>
#endif

#include "testrunnerswitcher.h"
#include "umock_c/umock_c.h"
#include "umock_c/umock_c_negative_tests.h"
#include "umock_c/umocktypes_bool.h"
#include "umock_c/umocktypes_stdint.h"

#if defined _MSC_VER
#pragma warning(disable: 4054) /* MSC incorrectly fires this */
#endif

static void* my_gballoc_malloc(size_t size)
{
    return malloc(size);
}

static void my_gballoc_free(void* ptr)
{
    free(ptr);
}

#define ENABLE_MOCKS

#include "azure_c_shared_utility/optimize_size.h"
#include "azure_c_shared_utility/tickcounter.h"
#include "azure_c_shared_utility/agenttime.h"
#include "azure_c_shared_utility/buffer_.h"
#include "azure_c_shared_utility/strings.h"
#include "azure_c_shared_utility/threadapi.h"

#include "azure_umqtt_c/internal/mqtt_codec_v5.h"
#include "azure_umqtt_c/mqtt_message.h"
#include "azure_c_shared_utility/gballoc.h"
#include "azure_c_shared_utility/platform.h"

#undef ENABLE_MOCKS

#include "azure_umqtt_c/mqtt_client_v5.h"
#include "azure_umqtt_c/mqttconst.h"

#define ENABLE_MOCKS
#include "umock_c/umock_c_prod.h"

//MOCKABLE_FUNCTION(, void, on_mqtt_operation_callback, MQTT_CLIENT_HANDLE, handle, MQTT_CLIENT_EVENT_RESULT, actionResult, const void*, msgInfo, void*, callbackCtx);
//MOCKABLE_FUNCTION(, void, on_mqtt_disconnected_callback, void*, callback_ctx);

#undef ENABLE_MOCKS

#ifdef __cplusplus
extern "C"
{
#endif

    int STRING_sprintf(STRING_HANDLE handle, const char* format, ...);
    STRING_HANDLE STRING_construct_sprintf(const char* format, ...);

#ifdef __cplusplus
}
#endif

MU_DEFINE_ENUM_STRINGS_2(QOS_VALUE, QOS_VALUE_VALUES);
TEST_DEFINE_ENUM_2_TYPE(QOS_VALUE, QOS_VALUE_VALUES);
IMPLEMENT_UMOCK_C_ENUM_2_TYPE(QOS_VALUE, QOS_VALUE_VALUES);

static const char* TEST_USERNAME = "testuser";
static const char* TEST_PASSWORD = "testpassword";

static BUFFER_HANDLE TEST_BUFFER_HANDLE = (BUFFER_HANDLE)0x15;

static const char* TEST_TOPIC_NAME = "topic Name";
static const APP_PAYLOAD TEST_APP_PAYLOAD = { (uint8_t*)"Message to send", 15 };
static const char* TEST_CLIENT_ID = "test_client_id";
static const char* TEST_WILL_MSG = "test_will_msg";
static const char* TEST_WILL_TOPIC = "test_will_topic";
static const char* TEST_SUBSCRIPTION_TOPIC = "subTopic";
static SUBSCRIBE_PAYLOAD TEST_SUBSCRIBE_PAYLOAD[] = { {"subTopic1", DELIVER_AT_LEAST_ONCE }, {"subTopic2", DELIVER_EXACTLY_ONCE } };
static const char* TEST_UNSUBSCRIPTION_TOPIC[] = { "subTopic1", "subTopic2" };

static const XIO_HANDLE TEST_IO_HANDLE = (XIO_HANDLE)0x11;
ON_PACKET_COMPLETE_CALLBACK g_packetComplete;
ON_IO_OPEN_COMPLETE g_openComplete;
ON_BYTES_RECEIVED g_bytesRecv;
ON_IO_ERROR g_ioError;
ON_SEND_COMPLETE g_sendComplete;
void* g_onCompleteCtx;
void* g_onSendCtx;
void* g_bytesRecvCtx;
void* g_ioErrorCtx;
static tickcounter_ms_t g_current_ms;
/*static const MQTT_CODEC_V3_HANDLE TEST_MQTTCODEC_HANDLE = (MQTT_CODEC_V3_HANDLE)0x13;
static const MQTT_MESSAGE_HANDLE TEST_MESSAGE_HANDLE = (MQTT_MESSAGE_HANDLE)0x14;
static const uint16_t TEST_KEEP_ALIVE_INTERVAL = 20;
static const uint16_t TEST_PACKET_ID = (uint16_t)0x1234;
static const unsigned char* TEST_BUFFER_U_CHAR = (const unsigned char*)0x19;

static bool g_operationCallbackInvoked;
static bool g_errorCallbackInvoked;
static bool g_msgRecvCallbackInvoked;
static bool g_codec_v3_publish_func_fail;
*/

typedef struct TEST_COMPLETE_DATA_INSTANCE_TAG
{
    //MQTT_CLIENT_EVENT_RESULT actionResult;
    void* msgInfo;
} TEST_COMPLETE_DATA_INSTANCE;

TEST_MUTEX_HANDLE test_serialize_mutex;

#define TEST_CONTEXT ((const void*)0x4242)
#define MAX_CLOSE_RETRIES               20
#define CLOSE_SLEEP_VALUE               2

#ifdef __cplusplus
extern "C" {
#endif

    static void mqtt_msg_recv_callback(MQTT_MESSAGE_HANDLE msg_handle, void* user_ctx)
    {
        (void)msg_handle;
        (void)user_ctx;
    }

    static MQTT_CODEC_V5_HANDLE my_codec_v5_create(ON_PACKET_COMPLETE_CALLBACK packetComplete, void* callContext)
    {
        (void)callContext;
        g_packetComplete = packetComplete;
        return (MQTT_CODEC_V5_HANDLE)my_gballoc_malloc(1);
    }

    static void my_codec_v5_destroy(MQTT_CODEC_V5_HANDLE handle)
    {
        my_gballoc_free(handle);
    }

    static int my_xio_open(XIO_HANDLE handle, ON_IO_OPEN_COMPLETE on_io_open_complete, void* on_io_open_complete_context, ON_BYTES_RECEIVED on_bytes_received, void* on_bytes_received_context, ON_IO_ERROR on_io_error, void* on_io_error_context)
    {
        (void)handle;
        /* Bug? : This is a bit wierd, why are we not using on_io_error and on_bytes_received? */
        g_openComplete = on_io_open_complete;
        g_onCompleteCtx = on_io_open_complete_context;
        g_bytesRecv = on_bytes_received;
        g_bytesRecvCtx = on_bytes_received_context;
        g_ioError = on_io_error;
        g_ioErrorCtx = on_io_error_context;
        return 0;
    }

    static int my_xio_send(XIO_HANDLE xio, const void* buffer, size_t size, ON_SEND_COMPLETE on_send_complete, void* callback_context)
    {
        (void)xio;
        (void)buffer;
        (void)size;
        g_sendComplete = on_send_complete;
        g_onSendCtx = callback_context;
        return 0;
    }

    static TICK_COUNTER_HANDLE my_tickcounter_create(void)
    {
        return (TICK_COUNTER_HANDLE)my_gballoc_malloc(1);
    }

    static void my_tickcounter_destroy(TICK_COUNTER_HANDLE tick_counter)
    {
        my_gballoc_free(tick_counter);
    }

    static int my_tickcounter_get_current_ms(TICK_COUNTER_HANDLE tick_counter, tickcounter_ms_t* current_ms)
    {
        (void)tick_counter;
        *current_ms = g_current_ms;
        return 0;
    }

    static int my_xio_close(XIO_HANDLE xio, ON_IO_CLOSE_COMPLETE on_io_close_complete, void* callback_context)
    {
        (void)xio;
        //if (on_io_close_complete != NULL)
        {
            on_io_close_complete(callback_context);
        }
        return 0;
    }

    static MQTT_MESSAGE_HANDLE my_mqttmessage_create(uint16_t packetId, const char* topicName, QOS_VALUE qosValue, const uint8_t* appMsg, size_t appMsgLength)
    {
        (void)packetId;
        (void)topicName;
        (void)qosValue;
        (void)appMsg;
        (void)appMsgLength;
        return (MQTT_MESSAGE_HANDLE)my_gballoc_malloc(1);
    }

    static MQTT_MESSAGE_HANDLE my_mqttmessage_create_in_place(uint16_t packetId, const char* topicName, QOS_VALUE qosValue, const uint8_t* appMsg, size_t appMsgLength)
    {
        (void)packetId;
        (void)topicName;
        (void)qosValue;
        (void)appMsg;
        (void)appMsgLength;
        return (MQTT_MESSAGE_HANDLE)my_gballoc_malloc(1);
    }

    static MQTT_MESSAGE_HANDLE my_mqttmessage_clone(MQTT_MESSAGE_HANDLE handle)
    {
        (void)handle;
        return (MQTT_MESSAGE_HANDLE)my_gballoc_malloc(1);
    }

    static void my_mqttmessage_destroy(MQTT_MESSAGE_HANDLE handle)
    {
        my_gballoc_free(handle);
    }

    static STRING_HANDLE my_STRING_new(void)
    {
        return (STRING_HANDLE)my_gballoc_malloc(1);
    }

    static void my_STRING_delete(STRING_HANDLE handle)
    {
        (void)handle;
        my_gballoc_free(handle);
    }

    int STRING_sprintf(STRING_HANDLE handle, const char* format, ...)
    {
        (void)handle;
        (void)format;
        return 0;
    }

    STRING_HANDLE STRING_construct_sprintf(const char* format, ...)
    {
        (void)format;
        return (STRING_HANDLE)my_gballoc_malloc(1);
    }

    static int TEST_mallocAndStrcpy_s(char** destination, const char* source)
    {
        size_t src_len = strlen(source);
        *destination = (char*)my_gballoc_malloc(src_len + 1);
        memcpy(*destination, source, src_len + 1);
        return 0;
    }

    static void codec_bytes_recieved(void* context, const unsigned char* buffer, size_t size)
    {
        (void)context;
        (void)buffer;
        (void)size;
    }

#ifdef __cplusplus
}
#endif

MU_DEFINE_ENUM_STRINGS(UMOCK_C_ERROR_CODE, UMOCK_C_ERROR_CODE_VALUES)

static void on_umock_c_error(UMOCK_C_ERROR_CODE error_code)
{
    ASSERT_FAIL("umock_c reported error :%s", MU_ENUM_TO_STRING(UMOCK_C_ERROR_CODE, error_code));
}

BEGIN_TEST_SUITE(mqtt_client_v5_ut)

TEST_SUITE_INITIALIZE(suite_init)
{
    int result;

    test_serialize_mutex = TEST_MUTEX_CREATE();
    ASSERT_IS_NOT_NULL(test_serialize_mutex);

    umock_c_init(on_umock_c_error);
    result = umocktypes_bool_register_types();
    ASSERT_ARE_EQUAL(int, 0, result);

    result = umocktypes_stdint_register_types();
    ASSERT_ARE_EQUAL(int, 0, result);

    REGISTER_UMOCK_ALIAS_TYPE(ON_PACKET_COMPLETE_CALLBACK, void*);
    REGISTER_UMOCK_ALIAS_TYPE(TICK_COUNTER_HANDLE, void*);
    //REGISTER_UMOCK_ALIAS_TYPE(MQTT_CODEC_V3_HANDLE, void*);
    REGISTER_UMOCK_ALIAS_TYPE(XIO_HANDLE, void*);
    REGISTER_UMOCK_ALIAS_TYPE(ON_SEND_COMPLETE, void*);
    REGISTER_UMOCK_ALIAS_TYPE(BUFFER_HANDLE, void*);
    REGISTER_UMOCK_ALIAS_TYPE(MQTT_MESSAGE_HANDLE, void*);
    REGISTER_UMOCK_ALIAS_TYPE(ON_IO_OPEN_COMPLETE, void*);
    REGISTER_UMOCK_ALIAS_TYPE(ON_BYTES_RECEIVED, void*);
    REGISTER_UMOCK_ALIAS_TYPE(ON_IO_ERROR, void*);
    REGISTER_UMOCK_ALIAS_TYPE(STRING_HANDLE, void*);
    REGISTER_UMOCK_ALIAS_TYPE(ON_IO_CLOSE_COMPLETE, void*)
    REGISTER_UMOCK_ALIAS_TYPE(TRACE_LOG_CALLBACK, void*)

    REGISTER_TYPE(QOS_VALUE, QOS_VALUE);

    REGISTER_GLOBAL_MOCK_HOOK(mallocAndStrcpy_s, TEST_mallocAndStrcpy_s);

    REGISTER_GLOBAL_MOCK_HOOK(gballoc_malloc, my_gballoc_malloc);
    REGISTER_GLOBAL_MOCK_FAIL_RETURN(gballoc_malloc, NULL);

    REGISTER_GLOBAL_MOCK_HOOK(gballoc_free, my_gballoc_free);

    REGISTER_GLOBAL_MOCK_HOOK(STRING_new, my_STRING_new);
    REGISTER_GLOBAL_MOCK_FAIL_RETURN(STRING_new, NULL);
    REGISTER_GLOBAL_MOCK_HOOK(STRING_delete, my_STRING_delete);
    REGISTER_GLOBAL_MOCK_RETURN(STRING_c_str, "Test");

    REGISTER_GLOBAL_MOCK_HOOK(xio_open, my_xio_open);
    REGISTER_GLOBAL_MOCK_FAIL_RETURN(xio_open, MU_FAILURE);
    REGISTER_GLOBAL_MOCK_HOOK(xio_send, my_xio_send);
    REGISTER_GLOBAL_MOCK_FAIL_RETURN(xio_send, MU_FAILURE);
    REGISTER_GLOBAL_MOCK_HOOK(xio_close, my_xio_close);

    REGISTER_GLOBAL_MOCK_HOOK(tickcounter_create, my_tickcounter_create);
    REGISTER_GLOBAL_MOCK_FAIL_RETURN(tickcounter_create, NULL);
    REGISTER_GLOBAL_MOCK_HOOK(tickcounter_get_current_ms, my_tickcounter_get_current_ms);
    REGISTER_GLOBAL_MOCK_FAIL_RETURN(tickcounter_get_current_ms, MU_FAILURE);
    REGISTER_GLOBAL_MOCK_HOOK(tickcounter_destroy, my_tickcounter_destroy);

    REGISTER_GLOBAL_MOCK_RETURN(get_time, time(NULL) );


    /*REGISTER_GLOBAL_MOCK_HOOK(codec_v3_create, my_codec_v3_create);
    REGISTER_GLOBAL_MOCK_RETURN(codec_v3_create, NULL);
    REGISTER_GLOBAL_MOCK_HOOK(codec_v3_destroy, my_codec_v3_destroy);
    REGISTER_GLOBAL_MOCK_HOOK(codec_v3_publishRelease, my_codec_v3_publishRelease);
    REGISTER_GLOBAL_MOCK_HOOK(codec_v3_publishComplete, my_codec_v3_publishComplete);
    REGISTER_GLOBAL_MOCK_RETURN(codec_v3_connect, TEST_BUFFER_HANDLE);

    REGISTER_GLOBAL_MOCK_RETURN(codec_v3_publish, TEST_BUFFER_HANDLE);
    REGISTER_GLOBAL_MOCK_FAIL_RETURN(codec_v3_publish, NULL);
    REGISTER_GLOBAL_MOCK_RETURN(codec_v3_subscribe, TEST_BUFFER_HANDLE);
    REGISTER_GLOBAL_MOCK_FAIL_RETURN(codec_v3_subscribe, NULL);
    REGISTER_GLOBAL_MOCK_RETURN(codec_v3_unsubscribe, TEST_BUFFER_HANDLE);
    REGISTER_GLOBAL_MOCK_FAIL_RETURN(codec_v3_unsubscribe, NULL);
    REGISTER_GLOBAL_MOCK_RETURN(codec_v3_disconnect, TEST_BUFFER_HANDLE);
    REGISTER_GLOBAL_MOCK_FAIL_RETURN(codec_v3_disconnect, NULL);
    REGISTER_GLOBAL_MOCK_RETURN(codec_v3_ping, TEST_BUFFER_HANDLE);
    REGISTER_GLOBAL_MOCK_FAIL_RETURN(codec_v3_ping, NULL);
    REGISTER_GLOBAL_MOCK_RETURN(codec_v3_publishAck, TEST_BUFFER_HANDLE);
    REGISTER_GLOBAL_MOCK_FAIL_RETURN(codec_v3_publishAck, NULL);
    REGISTER_GLOBAL_MOCK_RETURN(codec_v3_publishReceived, TEST_BUFFER_HANDLE);
    REGISTER_GLOBAL_MOCK_FAIL_RETURN(codec_v3_publishReceived, NULL);
    REGISTER_GLOBAL_MOCK_RETURN(codec_v3_get_recv_func, codec_bytes_recieved);

    REGISTER_GLOBAL_MOCK_RETURN(xio_close, 0);
    REGISTER_GLOBAL_MOCK_FAIL_RETURN(xio_close, MU_FAILURE);

    REGISTER_GLOBAL_MOCK_RETURN(platform_init, 0);
    REGISTER_GLOBAL_MOCK_FAIL_RETURN(platform_init, MU_FAILURE);

    REGISTER_GLOBAL_MOCK_RETURN(BUFFER_u_char, (unsigned char*)TEST_BUFFER_U_CHAR);
    REGISTER_GLOBAL_MOCK_RETURN(BUFFER_length, 11);

    REGISTER_GLOBAL_MOCK_HOOK(mqttmessage_create, my_mqttmessage_create);
    REGISTER_GLOBAL_MOCK_FAIL_RETURN(mqttmessage_create, NULL);
    REGISTER_GLOBAL_MOCK_HOOK(mqttmessage_create_in_place, my_mqttmessage_create_in_place);
    REGISTER_GLOBAL_MOCK_FAIL_RETURN(mqttmessage_create_in_place, NULL);
    REGISTER_GLOBAL_MOCK_HOOK(mqttmessage_clone, my_mqttmessage_clone);
    REGISTER_GLOBAL_MOCK_FAIL_RETURN(mqttmessage_clone, NULL);
    REGISTER_GLOBAL_MOCK_RETURN(mqttmessage_getPacketId, TEST_PACKET_ID);
    REGISTER_GLOBAL_MOCK_RETURN(mqttmessage_getTopicName, TEST_TOPIC_NAME);
    REGISTER_GLOBAL_MOCK_RETURN(mqttmessage_getQosType, DELIVER_AT_LEAST_ONCE);
    REGISTER_GLOBAL_MOCK_RETURN(mqttmessage_getIsDuplicateMsg, true);
    REGISTER_GLOBAL_MOCK_RETURN(mqttmessage_getIsRetained, true);
    REGISTER_GLOBAL_MOCK_RETURN(mqttmessage_setIsDuplicateMsg, 0);
    REGISTER_GLOBAL_MOCK_FAIL_RETURN(mqttmessage_setIsDuplicateMsg, MU_FAILURE);
    REGISTER_GLOBAL_MOCK_RETURN(mqttmessage_setIsRetained, 0);
    REGISTER_GLOBAL_MOCK_FAIL_RETURN(mqttmessage_setIsRetained, MU_FAILURE);
    REGISTER_GLOBAL_MOCK_RETURN(mqttmessage_getApplicationMsg, &TEST_APP_PAYLOAD);
    REGISTER_GLOBAL_MOCK_HOOK(mqttmessage_destroy, my_mqttmessage_destroy);*/

    REGISTER_GLOBAL_MOCK_RETURN(mallocAndStrcpy_s, 0);
    REGISTER_GLOBAL_MOCK_FAIL_RETURN(mallocAndStrcpy_s, MU_FAILURE);
}

TEST_SUITE_CLEANUP(suite_cleanup)
{
    umock_c_deinit();
    TEST_MUTEX_DESTROY(test_serialize_mutex);
}

TEST_FUNCTION_INITIALIZE(method_init)
{
    if (TEST_MUTEX_ACQUIRE(test_serialize_mutex))
    {
        ASSERT_FAIL("Could not acquire test serialization mutex.");
    }

    /*g_current_ms = 0;
    g_packetComplete = NULL;
    g_operationCallbackInvoked = false;
    g_errorCallbackInvoked = false;
    g_msgRecvCallbackInvoked = false;
    g_codec_v3_publish_func_fail = false;
    g_openComplete = NULL;
    g_onCompleteCtx = NULL;
    g_sendComplete = NULL;
    g_onSendCtx = NULL;
    g_bytesRecv = NULL;
    g_ioError = NULL;
    g_bytesRecvCtx = NULL;
    g_ioErrorCtx = NULL;*/
}

TEST_FUNCTION_CLEANUP(method_cleanup)
{
    umock_c_reset_all_calls();
    TEST_MUTEX_RELEASE(test_serialize_mutex);
}

static void setup_publish_callback_mocks(unsigned char* PUBLISH_RESP, size_t length, QOS_VALUE qos_value)
{
    (void)PUBLISH_RESP;
    (void)length;
    (void)qos_value;
    /*STRICT_EXPECTED_CALL(BUFFER_length(TEST_BUFFER_HANDLE)).SetReturn(length);
    STRICT_EXPECTED_CALL(BUFFER_u_char(TEST_BUFFER_HANDLE)).SetReturn(PUBLISH_RESP);
    EXPECTED_CALL(gballoc_malloc(IGNORED_NUM_ARG));
    STRICT_EXPECTED_CALL(mqttmessage_create_in_place(TEST_PACKET_ID, IGNORED_PTR_ARG, qos_value, IGNORED_PTR_ARG, TEST_APP_PAYLOAD.length));
    STRICT_EXPECTED_CALL(mqttmessage_setIsDuplicateMsg(IGNORED_PTR_ARG, true));
    STRICT_EXPECTED_CALL(mqttmessage_setIsRetained(IGNORED_PTR_ARG, true));
    STRICT_EXPECTED_CALL(codec_v3_publishReceived(TEST_PACKET_ID));
    EXPECTED_CALL(BUFFER_length(IGNORED_PTR_ARG)).SetReturn(length);
    EXPECTED_CALL(BUFFER_u_char(IGNORED_PTR_ARG)).SetReturn(PUBLISH_RESP);
    STRICT_EXPECTED_CALL(tickcounter_get_current_ms(TEST_COUNTER_HANDLE, IGNORED_PTR_ARG)).IgnoreArgument(2);
    EXPECTED_CALL(xio_send(IGNORED_PTR_ARG, IGNORED_PTR_ARG, IGNORED_NUM_ARG, IGNORED_PTR_ARG, IGNORED_PTR_ARG));
    EXPECTED_CALL(BUFFER_delete(IGNORED_PTR_ARG));
    STRICT_EXPECTED_CALL(mqttmessage_destroy(IGNORED_PTR_ARG));
    EXPECTED_CALL(gballoc_free(IGNORED_PTR_ARG));*/
}

static void setup_mqtt_clear_options_mocks(MQTT_CLIENT_OPTIONS* mqttOptions)
{
    if (mqttOptions->clientId != NULL)
    {
        EXPECTED_CALL(gballoc_free(IGNORED_PTR_ARG));
    }

    if (mqttOptions->willTopic != NULL)
    {
        EXPECTED_CALL(gballoc_free(IGNORED_PTR_ARG));
    }

    if (mqttOptions->willMessage != NULL)
    {
        EXPECTED_CALL(gballoc_free(IGNORED_PTR_ARG));
    }

    if (mqttOptions->username != NULL)
    {
        EXPECTED_CALL(gballoc_free(IGNORED_PTR_ARG));
    }

    if (mqttOptions->password != NULL)
    {
        EXPECTED_CALL(gballoc_free(IGNORED_PTR_ARG));
    }
}

static void setup_mqtt_client_disconnect_mocks(MQTT_CLIENT_OPTIONS* mqttOptions)
{
    //STRICT_EXPECTED_CALL(codec_v3_disconnect(IGNORED_PTR_ARG));
    STRICT_EXPECTED_CALL(BUFFER_length(TEST_BUFFER_HANDLE));
    STRICT_EXPECTED_CALL(BUFFER_u_char(TEST_BUFFER_HANDLE));
    STRICT_EXPECTED_CALL(tickcounter_get_current_ms(IGNORED_PTR_ARG, IGNORED_PTR_ARG)).IgnoreArgument(2);
    EXPECTED_CALL(xio_send(IGNORED_PTR_ARG, IGNORED_PTR_ARG, IGNORED_NUM_ARG, IGNORED_PTR_ARG, IGNORED_PTR_ARG));
    STRICT_EXPECTED_CALL(BUFFER_delete(TEST_BUFFER_HANDLE));

    setup_mqtt_clear_options_mocks(mqttOptions);
}

static void setup_mqtt_client_subscribe_mocks()
{
    //STRICT_EXPECTED_CALL(codec_v3_subscribe(IGNORED_PTR_ARG, TEST_PACKET_ID, TEST_SUBSCRIBE_PAYLOAD, 2));
    STRICT_EXPECTED_CALL(BUFFER_length(TEST_BUFFER_HANDLE));
    STRICT_EXPECTED_CALL(BUFFER_u_char(TEST_BUFFER_HANDLE));
    STRICT_EXPECTED_CALL(tickcounter_get_current_ms(IGNORED_PTR_ARG, IGNORED_PTR_ARG)).IgnoreArgument_current_ms();
    EXPECTED_CALL(xio_send(IGNORED_PTR_ARG, IGNORED_PTR_ARG, IGNORED_NUM_ARG, IGNORED_PTR_ARG, IGNORED_PTR_ARG));
    STRICT_EXPECTED_CALL(BUFFER_delete(TEST_BUFFER_HANDLE));
}

static void setup_mqtt_client_connect_mocks(MQTT_CLIENT_OPTIONS* mqttOptions)
{
    if (mqttOptions->clientId != NULL)
    {
        STRICT_EXPECTED_CALL(mallocAndStrcpy_s(IGNORED_PTR_ARG, mqttOptions->clientId));
    }

    if (mqttOptions->willTopic != NULL)
    {
        STRICT_EXPECTED_CALL(mallocAndStrcpy_s(IGNORED_PTR_ARG, mqttOptions->willTopic));
    }

    if (mqttOptions->willMessage != NULL)
    {
        STRICT_EXPECTED_CALL(mallocAndStrcpy_s(IGNORED_PTR_ARG, mqttOptions->willMessage));
    }

    if (mqttOptions->username != NULL)
    {
        STRICT_EXPECTED_CALL(mallocAndStrcpy_s(IGNORED_PTR_ARG, mqttOptions->username));
    }

    if (mqttOptions->password != NULL)
    {
        STRICT_EXPECTED_CALL(mallocAndStrcpy_s(IGNORED_PTR_ARG, mqttOptions->password));
    }

    /*STRICT_EXPECTED_CALL(codec_v3_get_recv_func());
    STRICT_EXPECTED_CALL(xio_open(TEST_IO_HANDLE, IGNORED_PTR_ARG, IGNORED_PTR_ARG, IGNORED_PTR_ARG, IGNORED_PTR_ARG, IGNORED_PTR_ARG, IGNORED_PTR_ARG));
    STRICT_EXPECTED_CALL(codec_v3_connect(IGNORED_PTR_ARG, IGNORED_PTR_ARG));
    STRICT_EXPECTED_CALL(BUFFER_length(TEST_BUFFER_HANDLE));
    STRICT_EXPECTED_CALL(BUFFER_u_char(TEST_BUFFER_HANDLE));
    STRICT_EXPECTED_CALL(tickcounter_get_current_ms(IGNORED_PTR_ARG, IGNORED_PTR_ARG));
    STRICT_EXPECTED_CALL(xio_send(IGNORED_PTR_ARG, IGNORED_PTR_ARG, IGNORED_NUM_ARG, IGNORED_PTR_ARG, IGNORED_PTR_ARG));
    STRICT_EXPECTED_CALL(BUFFER_delete(TEST_BUFFER_HANDLE));*/
}

static void setup_mqtt_client_connect_retry_mocks(MQTT_CLIENT_OPTIONS* mqttOptions)
{
    STRICT_EXPECTED_CALL(mallocAndStrcpy_s(IGNORED_PTR_ARG, TEST_CLIENT_ID));
    STRICT_EXPECTED_CALL(mallocAndStrcpy_s(IGNORED_PTR_ARG, TEST_WILL_MSG));
    STRICT_EXPECTED_CALL(mallocAndStrcpy_s(IGNORED_PTR_ARG, TEST_USERNAME));
    STRICT_EXPECTED_CALL(mallocAndStrcpy_s(IGNORED_PTR_ARG, TEST_PASSWORD));
    //STRICT_EXPECTED_CALL(codec_v3_get_recv_func());
    STRICT_EXPECTED_CALL(xio_open(TEST_IO_HANDLE, IGNORED_PTR_ARG, IGNORED_PTR_ARG, IGNORED_PTR_ARG, IGNORED_PTR_ARG, IGNORED_PTR_ARG, IGNORED_PTR_ARG))
        .SetReturn(MU_FAILURE);

    setup_mqtt_clear_options_mocks(mqttOptions);

    STRICT_EXPECTED_CALL(mallocAndStrcpy_s(IGNORED_PTR_ARG, TEST_CLIENT_ID));
    STRICT_EXPECTED_CALL(mallocAndStrcpy_s(IGNORED_PTR_ARG, TEST_WILL_MSG));
    STRICT_EXPECTED_CALL(mallocAndStrcpy_s(IGNORED_PTR_ARG, TEST_USERNAME));
    STRICT_EXPECTED_CALL(mallocAndStrcpy_s(IGNORED_PTR_ARG, TEST_PASSWORD));
    //STRICT_EXPECTED_CALL(codec_v3_get_recv_func());
    STRICT_EXPECTED_CALL(xio_open(IGNORED_PTR_ARG, IGNORED_PTR_ARG, IGNORED_PTR_ARG, IGNORED_PTR_ARG, IGNORED_PTR_ARG, IGNORED_PTR_ARG, IGNORED_PTR_ARG))
        .SetReturn(0);
}

static void TestRecvCallback(MQTT_MESSAGE_HANDLE msgHandle, void* context)
{
    (void)msgHandle;
    (void)context;
    //g_msgRecvCallbackInvoked = true;
}

static void test_operation_callback(MQTT_CLIENT_V5_HANDLE handle, MQTT_V5_CLIENT_EVENT_RESULT action_result, const void* msg_info, void* user_ctx)
{
    (void)action_result;
    (void)msg_info;
    (void)user_ctx;
    (void)handle;
    switch (action_result)
    {
        case 1:
            break;
        default:
            ASSERT_FAIL("Unexpected enum value: %d", action_result);
            break;

/*        case MQTT_CLIENT_ON_CONNACK:
        {
            if (context != NULL && msgInfo != NULL)
            {
                const CONNECT_ACK* connack = (CONNECT_ACK*)msgInfo;
                TEST_COMPLETE_DATA_INSTANCE* testData = (TEST_COMPLETE_DATA_INSTANCE*)context;
                CONNECT_ACK* validate = (CONNECT_ACK*)testData->msgInfo;
                if (connack->isSessionPresent == validate->isSessionPresent &&
                    connack->returnCode == validate->returnCode)
                {
                    g_operationCallbackInvoked = true;
                }
            }
            break;
        }
        case MQTT_CLIENT_ON_PUBLISH_ACK:
        case MQTT_CLIENT_ON_PUBLISH_RECV:
        case MQTT_CLIENT_ON_PUBLISH_REL:
        case MQTT_CLIENT_ON_PUBLISH_COMP:
        {
            if (context != NULL && msgInfo != NULL)
            {
                const PUBLISH_ACK* puback = (PUBLISH_ACK*)msgInfo;
                TEST_COMPLETE_DATA_INSTANCE* testData = (TEST_COMPLETE_DATA_INSTANCE*)context;
                PUBLISH_ACK* validate = (PUBLISH_ACK*)testData->msgInfo;
                if (testData->actionResult == actionResult && puback->packetId == validate->packetId)
                {
                    g_operationCallbackInvoked = true;
                }
            }
            break;
        }
        case MQTT_CLIENT_ON_SUBSCRIBE_ACK:
        {
            if (context != NULL && msgInfo != NULL)
            {
                const SUBSCRIBE_ACK* suback = (SUBSCRIBE_ACK*)msgInfo;
                TEST_COMPLETE_DATA_INSTANCE* testData = (TEST_COMPLETE_DATA_INSTANCE*)context;
                SUBSCRIBE_ACK* validate = (SUBSCRIBE_ACK*)testData->msgInfo;
                if (testData->actionResult == actionResult && validate->packetId == suback->packetId && validate->qosCount == suback->qosCount)
                {
                    for (size_t index = 0; index < suback->qosCount; index++)
                    {
                        if (suback->qosReturn[index] == validate->qosReturn[index])
                        {
                            g_operationCallbackInvoked = true;
                        }
                        else
                        {
                            g_operationCallbackInvoked = false;
                            break;
                        }
                    }
                }
            }
            break;
        }
        case MQTT_CLIENT_ON_UNSUBSCRIBE_ACK:
        {
            if (context != NULL && msgInfo != NULL)
            {
                const UNSUBSCRIBE_ACK* suback = (UNSUBSCRIBE_ACK*)msgInfo;
                TEST_COMPLETE_DATA_INSTANCE* testData = (TEST_COMPLETE_DATA_INSTANCE*)context;
                UNSUBSCRIBE_ACK* validate = (UNSUBSCRIBE_ACK*)testData->msgInfo;
                if (testData->actionResult == actionResult && validate->packetId == suback->packetId)
                {
                    g_operationCallbackInvoked = true;
                }
            }
            break;
        }
        case MQTT_CLIENT_ON_DISCONNECT:
        {
            if (msgInfo == NULL)
            {
                g_operationCallbackInvoked = true;
            }
            break;
        }
        case MQTT_CLIENT_ON_PING_RESPONSE:
        {
            if (msgInfo == NULL)
            {
                g_operationCallbackInvoked = true;
            }
            break;
        }*/
    }
}

static void test_error_callback(MQTT_V5_CLIENT_EVENT_ERROR error, void* user_ctx)
{
    (void)user_ctx;
    switch (error)
    {
        case 1:
            break;
        default:
            ASSERT_FAIL("Unexpected enum value: %d", error);
            break;

        /*case MQTT_CLIENT_CONNECTION_ERROR:
        case MQTT_CLIENT_PARSE_ERROR:
        case MQTT_CLIENT_MEMORY_ERROR:
        case MQTT_CLIENT_COMMUNICATION_ERROR:
        case MQTT_CLIENT_NO_PING_RESPONSE:
        case MQTT_CLIENT_UNKNOWN_ERROR:
        {
            //g_errorCallbackInvoked = true;
        }
        break;*/
    }
}

/*static void SetupMqttLibOptions(MQTT_CLIENT_OPTIONS* options, const char* clientId,
    const char* willMsg,
    const char* willTopic,
    const char* username,
    const char* password,
    uint16_t keepAlive,
    bool messageRetain,
    bool cleanSession,
    QOS_VALUE qos)
{
    options->clientId = (char*)clientId;
    options->willMessage = (char*)willMsg;
    options->willTopic = (char*)willTopic;
    options->username = (char*)username;
    options->password = (char*)password;
    options->keepAliveInterval = keepAlive;
    options->useCleanSession = cleanSession;
    options->qualityOfServiceValue = qos;
    options->messageRetain = messageRetain;
}

static void make_connack(MQTT_CLIENT_HANDLE mqttHandle, MQTT_CLIENT_OPTIONS* mqttOptions)
{
    (void)mqtt_client_connect(mqttHandle, TEST_IO_HANDLE, mqttOptions);

    unsigned char CONNACK_RESP[] = { 0x1, 0x0 };
    size_t length = sizeof(CONNACK_RESP) / sizeof(CONNACK_RESP[0]);
    BUFFER_HANDLE connack_handle = TEST_BUFFER_HANDLE;
    STRICT_EXPECTED_CALL(BUFFER_u_char(TEST_BUFFER_HANDLE)).SetReturn(CONNACK_RESP);
    STRICT_EXPECTED_CALL(BUFFER_length(TEST_BUFFER_HANDLE)).SetReturn(length);
    g_packetComplete(mqttHandle, CONNACK_TYPE, 0, connack_handle);
}*/

// mqttclient_connect
TEST_FUNCTION(mqtt_client_init_succeeds)
{
    // arrange
    EXPECTED_CALL(gballoc_malloc(IGNORED_NUM_ARG));
    STRICT_EXPECTED_CALL(tickcounter_create());
    EXPECTED_CALL(codec_v5_create(IGNORED_PTR_ARG, IGNORED_PTR_ARG));

    // act
    MQTT_CLIENT_V5_HANDLE result = mqtt_client_v5_create(mqtt_msg_recv_callback, test_operation_callback, NULL, test_error_callback, NULL);

    // assert
    ASSERT_IS_NOT_NULL(result);
    ASSERT_ARE_EQUAL(char_ptr, umock_c_get_expected_calls(), umock_c_get_actual_calls());

    // cleanup
    mqtt_client_v5_destroy(result);
}


END_TEST_SUITE(mqtt_client_v5_ut)
