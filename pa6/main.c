#define _POSIX_C_SOURCE 200809L

#include "common.h"
#include "ipc_private.h"
#include "pa2345.h"

#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

timestamp_t get_physical_time(void);

typedef struct {
    timestamp_t timestamp;
    local_id id;
} request_entry;

typedef struct {
    ipc_context ipc;
    int child_count;
    int mutex_enabled;
    int logical_time;
    request_entry requests[MAX_PROCESS_COUNT];
    int request_count;
    int waiting_for_replies;
    int reply_received[MAX_PROCESS_COUNT];
    int reply_count;
    int started[MAX_PROCESS_COUNT];
    int started_count;
    int done[MAX_PROCESS_COUNT];
    int done_count;
    int done_sent;
    int started_logged;
    int done_logged;
    int events_fd;
} process_state;

static int write_all(int fd, const char *data, size_t length)
{
    size_t written = 0;

    while (written < length) {
        ssize_t result = write(fd, data + written, length - written);

        if (result < 0) {
            if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) {
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

static int log_event(process_state *state, const char *line)
{
    size_t length = strlen(line);

    if (write_all(STDOUT_FILENO, line, length) != 0) {
        return -1;
    }
    return write_all(state->events_fd, line, length);
}

static int make_message(Message *msg, MessageType type, timestamp_t time,
                        const char *payload)
{
    size_t length = payload == NULL ? 0 : strlen(payload);

    if (length > MAX_PAYLOAD_LEN) {
        return -1;
    }
    memset(msg, 0, sizeof(*msg));
    msg->s_header.s_magic = MESSAGE_MAGIC;
    msg->s_header.s_payload_len = (uint16_t) length;
    msg->s_header.s_type = (int16_t) type;
    msg->s_header.s_local_time = time;
    if (length != 0) {
        memcpy(msg->s_payload, payload, length);
    }
    return 0;
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

static void close_unused_pipes(process_state *state)
{
    int from;
    int to;

    for (from = 0; from < state->ipc.process_count; ++from) {
        for (to = 0; to < state->ipc.process_count; ++to) {
            int keep_read;
            int keep_write;

            if (from == to) {
                continue;
            }
            keep_write = state->ipc.id != PARENT_ID && from == state->ipc.id;
            keep_read = to == state->ipc.id && from != PARENT_ID;
            if (!keep_read && state->ipc.pipes[from][to][0] >= 0) {
                close(state->ipc.pipes[from][to][0]);
                state->ipc.pipes[from][to][0] = -1;
            }
            if (!keep_write && state->ipc.pipes[from][to][1] >= 0) {
                close(state->ipc.pipes[from][to][1]);
                state->ipc.pipes[from][to][1] = -1;
            }
        }
    }
}

static int log_pipes(int fd,
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
                write_all(fd, line, (size_t) length) != 0) {
                return -1;
            }
        }
    }
    return 0;
}

static timestamp_t next_logical_time(process_state *state)
{
    ++state->logical_time;
    return (timestamp_t) state->logical_time;
}

static int request_order(timestamp_t left_time, local_id left_id,
                         timestamp_t right_time, local_id right_id)
{
    if (left_time != right_time) {
        return left_time < right_time;
    }
    return left_id < right_id;
}

static void insert_request(process_state *state, local_id id,
                           timestamp_t timestamp)
{
    int index;

    for (index = 0; index < state->request_count; ++index) {
        if (state->requests[index].id == id) {
            state->requests[index].timestamp = timestamp;
            break;
        }
    }
    if (index == state->request_count) {
        ++state->request_count;
    }
    state->requests[index].id = id;
    state->requests[index].timestamp = timestamp;

    while (index > 0 &&
           request_order(state->requests[index].timestamp,
                         state->requests[index].id,
                         state->requests[index - 1].timestamp,
                         state->requests[index - 1].id)) {
        request_entry temporary = state->requests[index - 1];
        state->requests[index - 1] = state->requests[index];
        state->requests[index] = temporary;
        --index;
    }
}

static void remove_request(process_state *state, local_id id)
{
    int index;

    for (index = 0; index < state->request_count; ++index) {
        if (state->requests[index].id == id) {
            int next;

            for (next = index + 1; next < state->request_count; ++next) {
                state->requests[next - 1] = state->requests[next];
            }
            --state->request_count;
            return;
        }
    }
}

static int send_control(process_state *state, local_id destination,
                        MessageType type, timestamp_t time)
{
    Message msg;

    if (make_message(&msg, type, time, NULL) != 0) {
        return -1;
    }
    return send(&state->ipc, destination, &msg);
}

static int log_received_all_started(process_state *state)
{
    char line[128];

    if (state->started_logged || state->ipc.id == PARENT_ID ||
        state->started_count != state->child_count - 1) {
        return 0;
    }
    if (snprintf(line, sizeof(line), log_received_all_started_fmt,
                 (int) get_physical_time(), (int) state->ipc.id) >=
        (int) sizeof(line)) {
        return -1;
    }
    if (log_event(state, line) != 0) {
        return -1;
    }
    state->started_logged = 1;
    return 0;
}

static int log_received_all_done(process_state *state)
{
    char line[128];

    if (state->done_logged || state->ipc.id == PARENT_ID || !state->done_sent ||
        state->done_count != state->child_count - 1) {
        return 0;
    }
    if (snprintf(line, sizeof(line), log_received_all_done_fmt,
                 (int) get_physical_time(), (int) state->ipc.id) >=
        (int) sizeof(line)) {
        return -1;
    }
    if (log_event(state, line) != 0) {
        return -1;
    }
    state->done_logged = 1;
    return 0;
}

static int handle_message(process_state *state, const Message *msg)
{
    local_id from = state->ipc.last_from;
    int type = msg->s_header.s_type;

    if (from <= PARENT_ID || from >= state->ipc.process_count) {
        return -1;
    }
    if (type == STARTED) {
        if (!state->started[from]) {
            state->started[from] = 1;
            ++state->started_count;
        }
        return log_received_all_started(state);
    }
    if (type == DONE) {
        if (!state->done[from]) {
            state->done[from] = 1;
            ++state->done_count;
        }
        return log_received_all_done(state);
    }
    if (type == CS_REQUEST) {
        timestamp_t remote_time = msg->s_header.s_local_time;

        if (remote_time > state->logical_time) {
            state->logical_time = remote_time;
        }
        next_logical_time(state);
        insert_request(state, from, remote_time);
        return send_control(state, from, CS_REPLY,
                            (timestamp_t) state->logical_time);
    }
    if (type == CS_REPLY) {
        if (state->waiting_for_replies && !state->reply_received[from]) {
            state->reply_received[from] = 1;
            ++state->reply_count;
        }
        return 0;
    }
    if (type == CS_RELEASE) {
        remove_request(state, from);
        return 0;
    }
    return -1;
}

static int receive_and_handle(process_state *state)
{
    Message msg;

    if (receive_any(&state->ipc, &msg) != 0) {
        return -1;
    }
    return handle_message(state, &msg);
}

int request_cs(const void *self)
{
    process_state *state = (process_state *) self;
    Message msg;
    timestamp_t timestamp;
    int destination;

    if (state == NULL || state->ipc.id == PARENT_ID ||
        !state->mutex_enabled) {
        return -1;
    }
    timestamp = next_logical_time(state);
    insert_request(state, state->ipc.id, timestamp);
    memset(state->reply_received, 0, sizeof(state->reply_received));
    state->reply_count = 0;
    state->waiting_for_replies = 1;
    if (make_message(&msg, CS_REQUEST, timestamp, NULL) != 0) {
        return -1;
    }
    for (destination = 1; destination < state->ipc.process_count;
         ++destination) {
        if (destination != state->ipc.id &&
            send(&state->ipc, (local_id) destination, &msg) != 0) {
            return -1;
        }
    }

    while (state->reply_count < state->child_count - 1 ||
           state->request_count == 0 ||
           state->requests[0].id != state->ipc.id) {
        if (receive_and_handle(state) != 0) {
            return -1;
        }
    }
    state->waiting_for_replies = 0;
    return 0;
}

int release_cs(const void *self)
{
    process_state *state = (process_state *) self;
    timestamp_t timestamp;
    int destination;

    if (state == NULL || state->ipc.id == PARENT_ID ||
        !state->mutex_enabled) {
        return -1;
    }
    remove_request(state, state->ipc.id);
    timestamp = next_logical_time(state);
    for (destination = 1; destination < state->ipc.process_count;
         ++destination) {
        if (destination != state->ipc.id &&
            send_control(state, (local_id) destination, CS_RELEASE,
                         timestamp) != 0) {
            return -1;
        }
    }
    return 0;
}

static int child_run(process_state *state)
{
    char line[256];
    Message msg;
    timestamp_t timestamp;
    int iteration;
    int iterations = (int) state->ipc.id * 5;

    timestamp = get_physical_time();
    if (snprintf(line, sizeof(line), log_started_fmt, (int) timestamp,
                 (int) state->ipc.id, (int) getpid(), (int) getppid(),
                 (int) state->ipc.id) >= (int) sizeof(line) ||
        log_event(state, line) != 0 ||
        make_message(&msg, STARTED, timestamp, line) != 0 ||
        send_multicast(&state->ipc, &msg) != 0) {
        return -1;
    }

    while (state->started_count < state->child_count - 1) {
        if (receive_and_handle(state) != 0) {
            return -1;
        }
    }
    if (log_received_all_started(state) != 0) {
        return -1;
    }

    for (iteration = 1; iteration <= iterations; ++iteration) {
        if (state->mutex_enabled && request_cs(state) != 0) {
            return -1;
        }
        if (snprintf(line, sizeof(line), log_loop_operation_fmt,
                     (int) state->ipc.id, iteration, iterations) >=
            (int) sizeof(line)) {
            return -1;
        }
        print(line);
        if (write_all(state->events_fd, line, strlen(line)) != 0) {
            return -1;
        }
        if (state->mutex_enabled && release_cs(state) != 0) {
            return -1;
        }
    }

    timestamp = get_physical_time();
    if (snprintf(line, sizeof(line), log_done_fmt, (int) timestamp,
                 (int) state->ipc.id, (int) state->ipc.id) >=
            (int) sizeof(line) ||
        log_event(state, line) != 0 ||
        make_message(&msg, DONE, timestamp, line) != 0 ||
        send_multicast(&state->ipc, &msg) != 0) {
        return -1;
    }
    state->done_sent = 1;
    if (log_received_all_done(state) != 0) {
        return -1;
    }
    while (!state->done_logged) {
        if (receive_and_handle(state) != 0) {
            return -1;
        }
    }
    return 0;
}

static int parent_run(process_state *state, const pid_t *children)
{
    int received_started = 0;
    int received_done = 0;
    int child;
    int failed = 0;

    while (received_started < state->child_count ||
           received_done < state->child_count) {
        int started_before = state->started_count;
        int done_before = state->done_count;

        if (receive_and_handle(state) != 0) {
            return -1;
        }
        received_started += state->started_count - started_before;
        received_done += state->done_count - done_before;
    }

    for (child = 0; child < state->child_count; ++child) {
        int status;
        pid_t result;

        do {
            result = waitpid(children[child], &status, 0);
        } while (result < 0 && errno == EINTR);
        if (result < 0 || !WIFEXITED(status) || WEXITSTATUS(status) != 0) {
            failed = 1;
        }
    }
    return failed ? -1 : 0;
}

static int parse_arguments(int argc, char **argv, int *child_count,
                           int *mutex_enabled)
{
    int index;
    int found_count = 0;

    *mutex_enabled = 0;
    for (index = 1; index < argc; ++index) {
        if (strcmp(argv[index], "--mutexl") == 0) {
            if (*mutex_enabled) {
                return -1;
            }
            *mutex_enabled = 1;
        } else if (strcmp(argv[index], "-p") == 0 && index + 1 < argc &&
                   !found_count) {
            char *end;
            long value;

            errno = 0;
            value = strtol(argv[++index], &end, 10);
            if (errno != 0 || end == argv[index] || *end != '\0' ||
                value < 1 || value > MAX_PROCESS_ID) {
                return -1;
            }
            *child_count = (int) value;
            found_count = 1;
        } else {
            return -1;
        }
    }
    return found_count ? 0 : -1;
}

int pa6_main(int argc, char **argv)
{
    int pipes[MAX_PROCESS_COUNT][MAX_PROCESS_COUNT][2];
    pid_t children[MAX_PROCESS_COUNT];
    process_state state;
    int child_count;
    int process_count;
    int pipes_fd;
    int from;
    int to;
    int spawned = 0;
    int failed = 0;

    memset(&state, 0, sizeof(state));
    if (parse_arguments(argc, argv, &child_count, &state.mutex_enabled) != 0) {
        fprintf(stderr, "Usage: %s -p X [--mutexl]\n", argv[0]);
        return 1;
    }
    state.child_count = child_count;
    process_count = child_count + 1;
    state.ipc.process_count = process_count;
    state.ipc.pipes = pipes;

    for (from = 0; from < MAX_PROCESS_COUNT; ++from) {
        for (to = 0; to < MAX_PROCESS_COUNT; ++to) {
            pipes[from][to][0] = -1;
            pipes[from][to][1] = -1;
        }
    }

    pipes_fd = open(pipes_log, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    state.events_fd = open(events_log,
                           O_WRONLY | O_CREAT | O_TRUNC | O_APPEND, 0644);
    if (pipes_fd < 0 || state.events_fd < 0) {
        perror("open");
        if (pipes_fd >= 0) {
            close(pipes_fd);
        }
        if (state.events_fd >= 0) {
            close(state.events_fd);
        }
        return 1;
    }

    for (from = 0; from < process_count; ++from) {
        for (to = 0; to < process_count; ++to) {
            int endpoint;

            if (from == to) {
                continue;
            }
            if (pipe(pipes[from][to]) != 0) {
                perror("pipe");
                close_all_pipes(pipes, process_count);
                close(pipes_fd);
                close(state.events_fd);
                return 1;
            }
            for (endpoint = 0; endpoint < 2; ++endpoint) {
                int flags = fcntl(pipes[from][to][endpoint], F_GETFL);

                if (flags < 0 || fcntl(pipes[from][to][endpoint], F_SETFL,
                                        flags | O_NONBLOCK) < 0) {
                    perror("fcntl");
                    close_all_pipes(pipes, process_count);
                    close(pipes_fd);
                    close(state.events_fd);
                    return 1;
                }
            }
        }
    }
    if (log_pipes(pipes_fd, pipes, process_count) != 0) {
        perror("pipes.log");
        close_all_pipes(pipes, process_count);
        close(pipes_fd);
        close(state.events_fd);
        return 1;
    }
    close(pipes_fd);
    signal(SIGPIPE, SIG_IGN);

    for (from = 1; from <= child_count; ++from) {
        pid_t child_pid = fork();

        if (child_pid < 0) {
            perror("fork");
            failed = 1;
            break;
        }
        if (child_pid == 0) {
            state.ipc.id = (local_id) from;
            if (dup2(STDOUT_FILENO, STDERR_FILENO) < 0) {
                _exit(1);
            }
            close_unused_pipes(&state);
            if (child_run(&state) != 0) {
                close_all_pipes(pipes, process_count);
                close(state.events_fd);
                _exit(1);
            }
            close_all_pipes(pipes, process_count);
            close(state.events_fd);
            _exit(0);
        }
        children[spawned++] = child_pid;
    }

    state.ipc.id = PARENT_ID;
    close_unused_pipes(&state);
    if (failed) {
        int index;

        for (index = 0; index < spawned; ++index) {
            kill(children[index], SIGTERM);
        }
        for (index = 0; index < spawned; ++index) {
            waitpid(children[index], NULL, 0);
        }
    } else if (parent_run(&state, children) != 0) {
        failed = 1;
    }

    close_all_pipes(pipes, process_count);
    close(state.events_fd);
    return failed ? 1 : 0;
}
