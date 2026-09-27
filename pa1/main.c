#define _POSIX_C_SOURCE 200809L

#include "common.h"
#include "ipc_private.h"
#include "pa1.h"

#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

static int write_all(int fd, const char *data, size_t length)
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

static int log_event(int events_fd, const char *line)
{
    size_t length = strlen(line);

    if (write_all(STDOUT_FILENO, line, length) != 0) {
        return -1;
    }
    return write_all(events_fd, line, length);
}

static void close_all_pipes(int pipes[MAX_PROCESS_COUNT][MAX_PROCESS_COUNT][2],
                            int process_count)
{
    int from;
    int to;

    for (from = 0; from < process_count; ++from) {
        for (to = 0; to < process_count; ++to) {
            if (from != to) {
                if (pipes[from][to][0] >= 0) {
                    close(pipes[from][to][0]);
                    pipes[from][to][0] = -1;
                }
                if (pipes[from][to][1] >= 0) {
                    close(pipes[from][to][1]);
                    pipes[from][to][1] = -1;
                }
            }
        }
    }
}

static void close_unused_pipes(ipc_context *context)
{
    int from;
    int to;

    for (from = 0; from < context->process_count; ++from) {
        for (to = 0; to < context->process_count; ++to) {
            int keep_read;
            int keep_write;

            if (from == to) {
                continue;
            }
            keep_write = context->id != PARENT_ID && from == context->id;
            keep_read = to == context->id && from != PARENT_ID;

            if (!keep_read && context->pipes[from][to][0] >= 0) {
                close(context->pipes[from][to][0]);
                context->pipes[from][to][0] = -1;
            }
            if (!keep_write && context->pipes[from][to][1] >= 0) {
                close(context->pipes[from][to][1]);
                context->pipes[from][to][1] = -1;
            }
        }
    }
}

static int create_message(Message *msg, MessageType type, const char *line)
{
    size_t length = strlen(line);

    if (length > MAX_PAYLOAD_LEN) {
        return -1;
    }
    memset(msg, 0, sizeof(*msg));
    msg->s_header.s_magic = MESSAGE_MAGIC;
    msg->s_header.s_payload_len = (uint16_t) length;
    msg->s_header.s_type = (int16_t) type;
    msg->s_header.s_local_time = 0;
    memcpy(msg->s_payload, line, length);
    return 0;
}

static int receive_type(ipc_context *context, local_id from,
                        MessageType expected_type)
{
    Message msg;

    if (receive(context, from, &msg) != 0 ||
        msg.s_header.s_type != (int16_t) expected_type) {
        return -1;
    }
    return 0;
}

static int child_run(ipc_context *context, int events_fd)
{
    char line[256];
    Message msg;
    int process_id;

    process_id = (int) context->id;
    if (snprintf(line, sizeof(line), log_started_fmt, process_id,
                 (int) getpid(), (int) getppid()) >= (int) sizeof(line) ||
        log_event(events_fd, line) != 0 ||
        create_message(&msg, STARTED, line) != 0 ||
        send_multicast(context, &msg) != 0) {
        return -1;
    }

    for (process_id = 1; process_id < context->process_count; ++process_id) {
        if (process_id != context->id &&
            receive_type(context, (local_id) process_id, STARTED) != 0) {
            return -1;
        }
    }
    if (snprintf(line, sizeof(line), log_received_all_started_fmt,
                 (int) context->id) >= (int) sizeof(line) ||
        log_event(events_fd, line) != 0) {
        return -1;
    }

    if (snprintf(line, sizeof(line), log_done_fmt, (int) context->id) >=
            (int) sizeof(line) ||
        log_event(events_fd, line) != 0 ||
        create_message(&msg, DONE, line) != 0 ||
        send_multicast(context, &msg) != 0) {
        return -1;
    }

    for (process_id = 1; process_id < context->process_count; ++process_id) {
        if (process_id != context->id &&
            receive_type(context, (local_id) process_id, DONE) != 0) {
            return -1;
        }
    }
    if (snprintf(line, sizeof(line), log_received_all_done_fmt,
                 (int) context->id) >= (int) sizeof(line) ||
        log_event(events_fd, line) != 0) {
        return -1;
    }
    return 0;
}

static int parse_child_count(int argc, char **argv, int *child_count)
{
    char *end;
    long value;

    if (argc != 3 || strcmp(argv[1], "-p") != 0) {
        return -1;
    }
    errno = 0;
    value = strtol(argv[2], &end, 10);
    if (errno != 0 || end == argv[2] || *end != '\0' || value < 1 ||
        value >= MAX_PROCESS_COUNT) {
        return -1;
    }
    *child_count = (int) value;
    return 0;
}

static int write_pipes_log(int pipes_fd,
                           int pipes[MAX_PROCESS_COUNT][MAX_PROCESS_COUNT][2],
                           int process_count)
{
    char line[128];
    int from;
    int to;

    for (from = 0; from < process_count; ++from) {
        for (to = 0; to < process_count; ++to) {
            int length;

            if (from == to) {
                continue;
            }
            length = snprintf(line, sizeof(line),
                              "Process %d -> %d: read fd %d, write fd %d\n",
                              from, to, pipes[from][to][0],
                              pipes[from][to][1]);
            if (length < 0 || length >= (int) sizeof(line) ||
                write_all(pipes_fd, line, (size_t) length) != 0) {
                return -1;
            }
        }
    }
    return 0;
}

static int parent_run(ipc_context *context, const pid_t *children,
                      int child_count)
{
    int child;
    int failed = 0;

    for (child = 1; child <= child_count; ++child) {
        if (receive_type(context, (local_id) child, STARTED) != 0) {
            failed = 1;
        }
    }
    for (child = 1; child <= child_count; ++child) {
        if (receive_type(context, (local_id) child, DONE) != 0) {
            failed = 1;
        }
    }

    child = 0;
    while (child < child_count) {
        int status;
        pid_t finished = wait(&status);

        if (finished < 0) {
            if (errno == EINTR) {
                continue;
            }
            failed = 1;
            break;
        }
        ++child;
        if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
            failed = 1;
        }
    }
    return failed ? -1 : 0;
}

int main(int argc, char **argv)
{
    int pipes[MAX_PROCESS_COUNT][MAX_PROCESS_COUNT][2];
    pid_t children[MAX_PROCESS_COUNT];
    int child_count;
    int process_count;
    int events_fd;
    int pipes_fd;
    int from;
    int to;
    int spawned = 0;
    int failed = 0;
    ipc_context context;

    if (parse_child_count(argc, argv, &child_count) != 0) {
        fprintf(stderr, "Usage: %s -p X\n", argv[0]);
        return 1;
    }
    process_count = child_count + 1;

    for (from = 0; from < MAX_PROCESS_COUNT; ++from) {
        for (to = 0; to < MAX_PROCESS_COUNT; ++to) {
            pipes[from][to][0] = -1;
            pipes[from][to][1] = -1;
        }
    }

    pipes_fd = open(pipes_log, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    events_fd = open(events_log, O_WRONLY | O_CREAT | O_TRUNC | O_APPEND, 0644);
    if (pipes_fd < 0 || events_fd < 0) {
        perror("open");
        if (pipes_fd >= 0) {
            close(pipes_fd);
        }
        if (events_fd >= 0) {
            close(events_fd);
        }
        return 1;
    }

    for (from = 0; from < process_count; ++from) {
        for (to = 0; to < process_count; ++to) {
            if (from != to && pipe(pipes[from][to]) != 0) {
                perror("pipe");
                close_all_pipes(pipes, process_count);
                close(pipes_fd);
                close(events_fd);
                return 1;
            }
        }
    }
    if (write_pipes_log(pipes_fd, pipes, process_count) != 0) {
        perror("pipes.log");
        close_all_pipes(pipes, process_count);
        close(pipes_fd);
        close(events_fd);
        return 1;
    }
    close(pipes_fd);

    for (from = 1; from <= child_count; ++from) {
        pid_t child_pid = fork();

        if (child_pid < 0) {
            perror("fork");
            failed = 1;
            break;
        }
        if (child_pid == 0) {
            context.id = (local_id) from;
            context.process_count = process_count;
            context.pipes = pipes;
            close_unused_pipes(&context);
            if (child_run(&context, events_fd) != 0) {
                close_all_pipes(pipes, process_count);
                close(events_fd);
                _exit(1);
            }
            close_all_pipes(pipes, process_count);
            close(events_fd);
            _exit(0);
        }
        children[spawned++] = child_pid;
    }

    context.id = PARENT_ID;
    context.process_count = process_count;
    context.pipes = pipes;
    close_unused_pipes(&context);

    if (failed) {
        int index;

        for (index = 0; index < spawned; ++index) {
            kill(children[index], SIGTERM);
        }
        for (index = 0; index < spawned; ++index) {
            waitpid(children[index], NULL, 0);
        }
    } else if (parent_run(&context, children, child_count) != 0) {
        failed = 1;
    }

    close_all_pipes(pipes, process_count);
    close(events_fd);
    return failed ? 1 : 0;
}
