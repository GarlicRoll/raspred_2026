#define _POSIX_C_SOURCE 200809L

#include "ipc_private.h"

#include <errno.h>
#include <fcntl.h>
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

static int write_all(int fd, const unsigned char *data, size_t length)
{
    size_t written = 0;

    while (written < length) {
        ssize_t result = write(fd, data + written, length - written);

        if (result < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        if (result == 0) {
            return -1;
        }
        written += (size_t) result;
    }
    return 0;
}

static int read_all(int fd, unsigned char *data, size_t length)
{
    size_t received = 0;

    while (received < length) {
        ssize_t result = read(fd, data + received, length - received);

        if (result < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        if (result == 0) {
            return -1;
        }
        received += (size_t) result;
    }
    return 0;
}

int send(void *self, local_id dst, const Message *msg)
{
    ipc_context *context = (ipc_context *) self;
    const unsigned char *data = (const unsigned char *) msg;
    size_t length;

    if (!valid_context(context) || msg == NULL || dst < 0 ||
        dst >= context->process_count || dst == context->id ||
        msg->s_header.s_magic != MESSAGE_MAGIC ||
        msg->s_header.s_payload_len > MAX_PAYLOAD_LEN) {
        return -1;
    }

    length = sizeof(MessageHeader) + msg->s_header.s_payload_len;
    return write_all(context->pipes[context->id][dst][1], data, length);
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

int receive(void *self, local_id from, Message *msg)
{
    ipc_context *context = (ipc_context *) self;
    unsigned char *data = (unsigned char *) msg;

    if (!valid_context(context) || msg == NULL || from < 0 ||
        from >= context->process_count || from == context->id) {
        return -1;
    }
    if (read_all(context->pipes[from][context->id][0], data,
                 sizeof(MessageHeader)) != 0) {
        return -1;
    }
    if (msg->s_header.s_magic != MESSAGE_MAGIC ||
        msg->s_header.s_payload_len > MAX_PAYLOAD_LEN) {
        return -1;
    }
    return read_all(context->pipes[from][context->id][0],
                    data + sizeof(MessageHeader),
                    msg->s_header.s_payload_len);
}

int receive_any(void *self, Message *msg)
{
    ipc_context *context = (ipc_context *) self;
    unsigned char buffers[MAX_PROCESS_COUNT][MAX_MESSAGE_LEN];
    size_t received[MAX_PROCESS_COUNT] = { 0 };
    size_t expected[MAX_PROCESS_COUNT] = { 0 };
    int original_flags[MAX_PROCESS_COUNT];
    int active[MAX_PROCESS_COUNT] = { 0 };
    int from;
    int result = -1;

    if (!valid_context(context) || msg == NULL) {
        return -1;
    }

    for (from = 0; from < context->process_count; ++from) {
        int fd;

        if (from == context->id) {
            continue;
        }
        fd = context->pipes[from][context->id][0];
        original_flags[from] = fcntl(fd, F_GETFL);
        if (original_flags[from] < 0 ||
            fcntl(fd, F_SETFL, original_flags[from] | O_NONBLOCK) < 0) {
            goto restore_flags;
        }
        active[from] = 1;
        expected[from] = sizeof(MessageHeader);
    }

    for (;;) {
        int made_progress = 0;

        for (from = 0; from < context->process_count; ++from) {
            int fd;
            ssize_t count;

            if (from == context->id) {
                continue;
            }
            fd = context->pipes[from][context->id][0];
            count = read(fd, buffers[from] + received[from],
                         expected[from] - received[from]);
            if (count > 0) {
                made_progress = 1;
                received[from] += (size_t) count;
                if (received[from] == sizeof(MessageHeader) &&
                    expected[from] == sizeof(MessageHeader)) {
                    const MessageHeader *header =
                        (const MessageHeader *) buffers[from];

                    if (header->s_magic != MESSAGE_MAGIC ||
                        header->s_payload_len > MAX_PAYLOAD_LEN) {
                        goto restore_flags;
                    }
                    expected[from] = sizeof(MessageHeader) +
                                     header->s_payload_len;
                }
                if (received[from] == expected[from]) {
                    memset(msg, 0, sizeof(*msg));
                    memcpy(msg, buffers[from], expected[from]);
                    result = 0;
                    goto restore_flags;
                }
            } else if (count == 0) {
                goto restore_flags;
            } else if (errno != EAGAIN && errno != EWOULDBLOCK &&
                       errno != EINTR) {
                goto restore_flags;
            }
        }

        if (!made_progress) {
            struct timespec delay = { 0, 1000000L };
            nanosleep(&delay, NULL);
        }
    }

restore_flags:
    for (from = 0; from < context->process_count; ++from) {
        if (active[from]) {
            int fd = context->pipes[from][context->id][0];
            fcntl(fd, F_SETFL, original_flags[from]);
        }
    }
    return result;
}
