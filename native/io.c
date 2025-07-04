#include <stdio.h>
#include <liburing.h>
#include <unistd.h>

typedef struct {
    unsigned char* buffer;
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
