#ifndef FP_UTILS_H
#define FP_UTILS_H

#include "errors.c"

FILE* safe_file_write(const char *path) {
	bool create_mode = access(path, F_OK) == -1;
    FILE *fp = fopen(path, "wb");
    if (fp == NULL) {
        char *msg = (char*) malloc(strlen(path) + 200);
        strcpy(msg, "Failed to open file for writing: ");
        strcat(msg, path);
        err_exit(msg);
    }
    if (create_mode) {
		if (chmod(path, 0707) != 0) {
			char *msg = (char*) malloc(strlen(path) + 200);
			strcpy(msg, "Failed to set file permissions CFW: ");
			strcat(msg, path);
			err_exit(msg);
		}
	}
    return fp;
}

FILE* safe_file_read(const char *path) {
    FILE *fp = fopen(path, "rb");
    if (fp == NULL) {
        char *msg = (char*) malloc(strlen(path) + 200);
        strcpy(msg, "Failed to open file for reading: ");
        strcat(msg, path);
        err_exit(msg);
    }
    return fp;
}

#endif
