#include <stdint.h>
#include <stdio.h>
#include <liburing.h>
#include <unistd.h>

typedef struct {
    const unsigned char* buffer;
    size_t length;
    off_t offset; 
} WriteEntry;

int batch_write_data_c(WriteEntry* entries, size_t len, const int file) {
    struct io_uring ring;
    if (io_uring_queue_init(len + 1, &ring, 0) < 0) {
        perror("io_uring_queue_init");
        return -1;
    }

    for (size_t index = 0; index < len; index++) {
        struct io_uring_sqe* sqe = io_uring_get_sqe(&ring);
        if (!sqe) {
            fprintf(stderr, "No submission queue entry available\n");
            io_uring_queue_exit(&ring);
            return -1;
        }
        WriteEntry en = entries[index];
        io_uring_prep_write(sqe, file, en.buffer, en.length, en.offset);
    }

    struct io_uring_sqe* sqe = io_uring_get_sqe(&ring);
    if (!sqe) {
        fprintf(stderr, "No submission queue entry for fsync\n");
        io_uring_queue_exit(&ring);
        return -1;
    }

    io_uring_prep_fsync(sqe, file, 0);

    int submitted = io_uring_submit(&ring);
    if (submitted < 0) {
        perror("io_uring_submit");
        io_uring_queue_exit(&ring);
        return -1;
    }

    for (size_t i = 0; i < len + 1; i++) {
        struct io_uring_cqe* cqe;
        int ret = io_uring_wait_cqe(&ring, &cqe);
        if (ret < 0) {
            perror("io_uring_wait_cqe");
            io_uring_queue_exit(&ring);
            return -1;
        }
        if (cqe->res < 0) {
            fprintf(stderr, "Async operation failed: %d\n", cqe->res);
            io_uring_cqe_seen(&ring, cqe);
            io_uring_queue_exit(&ring);
            return -1;
        }
        io_uring_cqe_seen(&ring, cqe);
    }
    io_uring_queue_exit(&ring);
    return 0;
}


struct ReadInstance{
    uint64_t size;
    unsigned char* buffer;
    uint64_t offset;
};

struct ReadEntry{
    uint64_t len;
    struct ReadInstance* buffer_array;
};

/* Error codes for batch_reads
 *
 * -1 : failed to get SQE
 * -2 : failed to init queue
 * -3 : failed to submit io_uring_submit
 *  -4 : failed
 * */

int batch_reads(struct ReadEntry* re, int file){
    struct io_uring ring;

    if (!io_uring_queue_init(re->len + 1,&ring, 0)){
        return -2;
    }
    
    const uint64_t entry_size = re->len;
    for (uint64_t index; index<entry_size; index++){
        struct io_uring_sqe* sqe = io_uring_get_sqe(&ring);
        if (!sqe){
            io_uring_queue_exit(&ring);
            return -1;
        }
        io_uring_prep_read(sqe,file,re->buffer_array[index].buffer,re->buffer_array[index].size,re->buffer_array[index].offset);
    }

    if (!io_uring_submit(&ring)){
        io_uring_queue_exit(&ring);
        return -3;
    }
    for (uint64_t index; index < re->len + 1; index ++) {
        struct io_uring_cqe* cqe;
        int abc = io_uring_wait_cqe(&ring, &cqe);
        if (!abc){
            io_uring_queue_exit(&ring);
            return -4;
        }
    }
    io_uring_queue_exit(&ring);
    return 0;
}















