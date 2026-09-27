#ifndef PA6_IPC_PRIVATE_H
#define PA6_IPC_PRIVATE_H

#include "ipc.h"

enum { MAX_PROCESS_COUNT = MAX_PROCESS_ID + 1 };

typedef struct {
    local_id id;
    int process_count;
    int (*pipes)[MAX_PROCESS_COUNT][2];
    unsigned char input[MAX_PROCESS_COUNT][MAX_MESSAGE_LEN];
    size_t received[MAX_PROCESS_COUNT];
    size_t expected[MAX_PROCESS_COUNT];
    local_id last_from;
} ipc_context;

#endif

