#ifndef _DALVIK_AGATE_UTIL
#define _DALVIK_AGATE_UTIL

char* _agate_util_int_to_byte_array(char* dest, int value);
int _agate_util_int_from_byte_array(char* bytes);
char* _agate_util_add_int(char* dest, int val);
char* _agate_util_get_int(char* dest, int* val);
char* _agate_util_read_x_bytes_from_socket(int sockfd, int len);
void _agate_util_write_x_bytes_to_socket(int sockfd, char* buffer, int len);

#endif /*_DALVIK_AGATE_UTIL */
