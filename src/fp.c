#ifndef FP_H
#define FP_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "paths.c"

FILE *JOB_QUEUE = NULL;
FILE *JOB_FILE = NULL;
FILE *JOB_END_FILE = NULL;
FILE *QREL_FILE = NULL;

FILE* safe_file_write(const char *path) {
    FILE *fp = fopen(path, "wb");
    if (fp == NULL) {
        char *msg = (char*) malloc(strlen(path) + 200);
        strcpy(msg, "Failed to open file for writing: ");
        strcat(msg, path);
        err_exit(msg);
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


FILE *read_job_file() {
    if (JOB_FILE == NULL) {
		JOB_FILE = safe_file_read(job_start_path_buffer().buffer);
	}
    return JOB_FILE;
}

FILE *read_job_end_file() {
    if (JOB_END_FILE == NULL)
        JOB_END_FILE = safe_file_read(job_end_fifo_buffer().buffer);
    return JOB_END_FILE;
}

FILE *write_job_file() {
    if (JOB_FILE == NULL)
		JOB_FILE = safe_file_write(job_start_path_buffer().buffer);
    return JOB_FILE;
}

FILE *write_job_end_file() {
    if (JOB_END_FILE == NULL)
		JOB_END_FILE = safe_file_write(job_end_fifo_buffer().buffer);
    return JOB_END_FILE;
}

FILE *read_job_queue() {
    if (JOB_QUEUE == NULL)
        JOB_QUEUE = safe_file_read(job_queue());
    return JOB_QUEUE;
}

FILE *read_qrel_file() {
	if (QREL_FILE == NULL) {
		char *qrel_file_path = qrel_file();
		QREL_FILE = safe_file_read(qrel_file_path);
	}
	return QREL_FILE;
}

FILE *write_qrel_file() {
	if (QREL_FILE == NULL) {
		char *qrel_file_path = qrel_file();
		QREL_FILE = safe_file_write(qrel_file_path);
	}
	return QREL_FILE;
}

void close_qrel_file() {
	if (QREL_FILE != NULL) {
		fclose(QREL_FILE);
		QREL_FILE = NULL;
	}
}


#endif
