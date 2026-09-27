#define _POSIX_C_SOURCE 200809L

#include "ipc_private.h"

#include <errno.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

static int valid_context(const ipc_context *context)
{
    return context != NULL && context->pipes != NULL &&
           context->process_count > 0 &&
           context->process_count <= MAX_PROCESS_COUNT &&
           context->id >= 0 && context->id < context->process_count;
}

static void short_delay(void)
{
    struct timespec delay = { 0, 1000000L };
    nanosleep(&delay, NULL);
}

int send(void *self, local_id dst, const Message *msg)
{
    ipc_context *context = (ipc_context *) self;
    unsigned char frame[MAX_MESSAGE_LEN];
    size_t length;
    int fd;

    if (!valid_context(context) || msg == NULL || dst < 0 ||
        dst >= context->process_count || dst == context->id ||
        msg->s_header.s_magic != MESSAGE_MAGIC ||
        msg->s_header.s_payload_len > MAX_PAYLOAD_LEN) {
        return -1;
    }

    length = sizeof(MessageHeader) + msg->s_header.s_payload_len;
    memcpy(frame, msg, length);
    fd = context->pipes[context->id][dst][1];
    for (;;) {
        ssize_t result = write(fd, frame, length);

        if (result == (ssize_t) length) {
            return 0;
        }
        if (result < 0 && (errno == EINTR || errno == EAGAIN ||
                           errno == EWOULDBLOCK)) {
            short_delay();
            continue;
        }
        return -1;
    }
}

int send_multicast(void *self, const Message *msg)
{
    ipc_context *context = (ipc_context *) self;
    int dst;

    if (!valid_context(context) || msg == NULL) {
        return -1;
    }
    for (dst = 0; dst < context->process_count; ++dst) {
        if (dst != context->id && send(context, (local_id) dst, msg) != 0) {
            return -1;
        }
    }
    return 0;
}

static int read_from(ipc_context *context, local_id from, Message *msg)
{
    int fd = context->pipes[from][context->id][0];

    if (context->expected[from] == 0) {
        context->expected[from] = sizeof(MessageHeader);
    }
    for (;;) {
        ssize_t result = read(fd, context->input[from] + context->received[from],
                              context->expected[from] - context->received[from]);

        if (result > 0) {
            const MessageHeader *header;

            context->received[from] += (size_t) result;
            if (context->received[from] == sizeof(MessageHeader) &&
                context->expected[from] == sizeof(MessageHeader)) {
                header = (const MessageHeader *) context->input[from];
                if (header->s_magic != MESSAGE_MAGIC ||
                    header->s_payload_len > MAX_PAYLOAD_LEN) {
                    return -1;
                }
                context->expected[from] = sizeof(MessageHeader) +
                                          header->s_payload_len;
            }
            if (context->received[from] == context->expected[from]) {
                memset(msg, 0, sizeof(*msg));
                memcpy(msg, context->input[from], context->expected[from]);
                context->received[from] = 0;
                context->expected[from] = 0;
                context->last_from = from;
                return 1;
            }
        } else if (result == 0) {
            return -1;
        } else if (errno == EINTR) {
            continue;
        } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return 0;
        } else {
            return -1;
        }
    }
}

int receive(void *self, local_id from, Message *msg)
{
    ipc_context *context = (ipc_context *) self;

    if (!valid_context(context) || msg == NULL || from < 0 ||
        from >= context->process_count || from == context->id ||
        (context->id != PARENT_ID && from == PARENT_ID)) {
        return -1;
    }
    for (;;) {
        int result = read_from(context, from, msg);

        if (result != 0) {
            return result < 0 ? -1 : 0;
        }
        short_delay();
    }
}


int receive_any(void *self, Message *msg)
{
    ipc_context *context = (ipc_context *) self;

    if (!valid_context(context) || msg == NULL) {
        return -1;
    }
    for (;;) {
        int from;

        for (from = 0; from < context->process_count; ++from) {
            int result;

            if (from == context->id ||
                (context->id != PARENT_ID && from == PARENT_ID)) {
                continue;
            }
            result = read_from(context, (local_id) from, msg);
            if (result < 0) {
                return -1;
            }
            if (result > 0) {
                return 0;
            }
        }
        short_delay();
    }
}
