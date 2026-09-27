#ifndef PA1_IPC_PRIVATE_H
#define PA1_IPC_PRIVATE_H

#include "ipc.h"

enum { MAX_PROCESS_COUNT = MAX_PROCESS_ID + 1 };

typedef struct {
    local_id id;
    int process_count;
    int (*pipes)[MAX_PROCESS_COUNT][2];
} ipc_context;

#endif
