#include <stdio.h>

int write_data(unsigned char* buffer, size_t len, const char* path) {
    FILE *file = fopen(path, "wb"); 
    if (file == NULL) {
        return 0;
    }
    size_t written = fwrite(buffer, sizeof(unsigned char), len, file);
    fclose(file);  
    return written == len; 
}