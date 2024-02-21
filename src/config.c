#include <string.h>

char *CACHE_DIR_PATH = NULL;
char *JOB_DIR_PATH = NULL;
char *JOB_QUEUE_PATH = NULL;
char *QREL_DIR = NULL;
char *DOC_ID_DIR_PATH = NULL;

char *cache_dir_path() {
	if (CACHE_DIR_PATH != NULL)
		return CACHE_DIR_PATH;
	char *home = getenv("HOME");
	CACHE_DIR_PATH = (char*) malloc(strlen(home) + 12);
	strcpy(CACHE_DIR_PATH, home);
	strcat(CACHE_DIR_PATH, "/.cache/");
	struct stat st = {0};
	if (stat(CACHE_DIR_PATH, &st) == -1) mkdir(CACHE_DIR_PATH, 0700);
	strcat(CACHE_DIR_PATH, "lrr/");
	if (stat(CACHE_DIR_PATH, &st) == -1) mkdir(CACHE_DIR_PATH, 0700);
	return CACHE_DIR_PATH;
}

char *job_start_dir() {
	if (JOB_DIR_PATH != NULL)
		return JOB_DIR_PATH;
	char *cache_dir = cache_dir_path();
	JOB_DIR_PATH = (char*) malloc(strlen(cache_dir) + 11);
	strcpy(JOB_DIR_PATH, cache_dir);
	strcat(JOB_DIR_PATH, "job_start/");
	struct stat st = {0};
	if (stat(JOB_DIR_PATH, &st) == -1) mkdir(JOB_DIR_PATH, 0700);
	return JOB_DIR_PATH;
}

char *job_end_dir() {
	if (DOC_ID_DIR_PATH != NULL)
		return DOC_ID_DIR_PATH;
	char *cache_dir = cache_dir_path();
	DOC_ID_DIR_PATH = (char*) malloc(strlen(cache_dir) + 9);
	strcpy(DOC_ID_DIR_PATH, cache_dir);
	strcat(DOC_ID_DIR_PATH, "job_end/");
	struct stat st = {0};
	if (stat(DOC_ID_DIR_PATH, &st) == -1) mkdir(DOC_ID_DIR_PATH, 0700);
	return DOC_ID_DIR_PATH;
}

char *qrel_dir() {
	if (QREL_DIR != NULL)
		return QREL_DIR;
	char *cache_dir = cache_dir_path();
	QREL_DIR = (char*) malloc(strlen(cache_dir) + 6);
	strcpy(QREL_DIR, cache_dir);
	strcat(QREL_DIR, "qrel/");
	struct stat st = {0};
	if (stat(QREL_DIR, &st) == -1) mkdir(QREL_DIR, 0700);
	return QREL_DIR;
}

char *job_queue() {
	if (JOB_QUEUE_PATH != NULL)
		return JOB_QUEUE_PATH;
	char *cache_dir = cache_dir_path();
	JOB_QUEUE_PATH = (char*) malloc(strlen(cache_dir) + 10);
	strcpy(JOB_QUEUE_PATH, cache_dir);
	strcat(JOB_QUEUE_PATH, "job_queue");
	struct stat st = {0};
	if (stat(JOB_QUEUE_PATH, &st) == -1)
		mkfifo(JOB_QUEUE_PATH, 0600);
	return JOB_QUEUE_PATH;
}

