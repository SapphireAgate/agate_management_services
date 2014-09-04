#include<stdio.h>
#include<string.h>
#include<stdlib.h>
#include<sys/socket.h>
#include<errno.h>
#include <arpa/inet.h>
#include <unistd.h>


/**
 *  Helper functions
 */
char* _agate_util_int_to_byte_array(char* dest, int value) {
    for (unsigned int i = 0; i < sizeof(int); i++) {
        *dest++ = (char)((value >> ((sizeof(int) - i - 1) * 8)) & 0xff);
    }
    return dest;
}

int _agate_util_int_from_byte_array(char* bytes) {
    int value = 0;
    for (unsigned int i = 0; i < sizeof(int); i++) {
        value = value << 8;
        value |= bytes[i] & 0xff;
    }
    return value;
}

char* _agate_util_add_int(char* dest, int val) {
    return _agate_util_int_to_byte_array(dest, val);
}

char* _agate_util_get_int(char* dest, int* val) {
    *val = _agate_util_int_from_byte_array(dest); 
    return dest + sizeof(int);
}

/* Returns null terminated char* */
char* _agate_util_read_x_bytes_from_socket(int sockfd, int len) {
    int r = len;
    int res;
    char* buffer = (char*)malloc(len + 1); // null terminated
    buffer[len] = '\0';

    while (r > 0) {
        res = read(sockfd, buffer + len - r, r);
        if (res == -1) {
            fprintf(stderr, "AgateLog: [_agate_util_read_x_bytes_from_socket] error: reading from socket, errno: %d.\n", errno);
            return NULL;
        } else if (res == 0) {
            if (r < len)
                fprintf(stderr, "AgateLog: [_agate_util_read_x_bytes_from_socket] error: read 0 bytes while in the middle of reading the message.\n");
            return NULL;
        }
        r -= res;
    }
    return buffer;
}

int _agate_util_write_x_bytes_to_socket(int sockfd, char* buffer, int len) {
    int r = len;
    int res;

    while (r > 0) {
        res = write(sockfd, buffer + len - r, r);
        if (res == -1) {
            fprintf(stderr, "AgateLog: [_agate_util_write_x_bytes_to_socket] error: writing to socket, errno: %d.\n", errno);
            return 0;
        } else if (res == 0) {
            if (r < len)
                fprintf(stderr, "UMS: error: wrote 0 bytes while not completing the size of the command message.\n");
            return 0;
        }
        r -= res;
    }
    return 1;
}

