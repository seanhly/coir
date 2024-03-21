#ifndef CONFIG_C
#define CONFIG_C

#include <string.h>

typedef struct {
	percentage CM_confidence;
	ui8 logging_level;
} Configuration;

const Configuration DEFAULT_CONFIG = {
	.CM_confidence = 95
};

#include "config_parser.c"

char *VAR_DIR_PATH = NULL;
char *CACHE_DIR_PATH = NULL;
char *JOB_DIR_PATH = NULL;
char *JOB_QUEUE_PATH = NULL;
char *QREL_DIR = NULL;
char *DOC_ID_DIR_PATH = NULL;
char *CONFIG_DIR_PATH = NULL;
char *CONFIG_FILE_PATH = NULL;
Configuration CONFIGURATION;
bool CONFIG_INITIALIZED = false;

char *var_dir_path() {
	if (VAR_DIR_PATH == NULL) {
		VAR_DIR_PATH = (char*) malloc(20);
		strcpy(VAR_DIR_PATH, "/var/");
		struct stat st = {0};
		if (stat(VAR_DIR_PATH, &st) == -1) mkdir(VAR_DIR_PATH, 0700);
		strcat(VAR_DIR_PATH, "lib/");
		if (stat(VAR_DIR_PATH, &st) == -1) mkdir(VAR_DIR_PATH, 0700);
		strcat(VAR_DIR_PATH, "librecoir/");
		if (stat(VAR_DIR_PATH, &st) == -1) mkdir(VAR_DIR_PATH, 0707);
	}
	return VAR_DIR_PATH;
}

char *config_dir_path() {
	if (CONFIG_DIR_PATH == NULL) {
		char *config_home = getenv("XDG_CONFIG_HOME");
		if (config_home == NULL) {
			char *home = getenv("HOME");
			config_home = (char*) malloc(strlen(home) + 14);
			strcpy(config_home, home);
			strcat(config_home, "/.config/");
		}
		CONFIG_DIR_PATH = config_home;
	}
	return CONFIG_DIR_PATH;
}

char *config_file_path() {
	if (CONFIG_FILE_PATH == NULL) {
		char *config_dir = config_dir_path();
		struct stat st = {0};
		if (stat(config_dir, &st) == -1)
			mkdir(config_dir, 0700);
		char *config_file = (char*) malloc(strlen(config_dir) + 15);
		strcpy(config_file, config_dir);
		strcat(config_file, "librecoir.ini");
		CONFIG_FILE_PATH = config_file;
	}
	return CONFIG_FILE_PATH;
}

Configuration config() {
	if (!CONFIG_INITIALIZED) {
		// Check if file exists
		struct stat st;
		if (stat(config_file_path(), &st) == -1) {
			FILE *config_file = safe_file_write(config_file_path());
			fprintf(config_file, "[click-models]\n");
			fprintf(config_file, "confidence = 0.95\n");
			fprintf(config_file, "\n");
			fprintf(config_file, "[logging]\n");
			fprintf(config_file, "level = 0\n");
			fclose(config_file);
		}
		FILE *config_file = safe_file_read(config_file_path());
		CONFIGURATION = parse_config_file(config_file);
		CONFIG_INITIALIZED = true;
	}
	return CONFIGURATION;
}

char *job_start_dir() {
	if (JOB_DIR_PATH == NULL) {
		char *var_dir = var_dir_path();
		JOB_DIR_PATH = (char*) malloc(strlen(var_dir) + 11);
		strcpy(JOB_DIR_PATH, var_dir);
		strcat(JOB_DIR_PATH, "job_start/");
		struct stat st = {0};
		if (stat(JOB_DIR_PATH, &st) == -1) mkdir(JOB_DIR_PATH, 0707);
	}

	return JOB_DIR_PATH;
}

char *job_end_dir() {
	if (DOC_ID_DIR_PATH == NULL) {
		char *var_dir = var_dir_path();
		DOC_ID_DIR_PATH = (char*) malloc(strlen(var_dir) + 9);
		strcpy(DOC_ID_DIR_PATH, var_dir);
		strcat(DOC_ID_DIR_PATH, "job_end/");
		struct stat st = {0};
		if (stat(DOC_ID_DIR_PATH, &st) == -1) mkdir(DOC_ID_DIR_PATH, 0705);
	}
	return DOC_ID_DIR_PATH;
}

char *qrel_dir() {
	if (QREL_DIR == NULL) {
		char *var_dir = var_dir_path();
		QREL_DIR = (char*) malloc(strlen(var_dir) + 6);
		strcpy(QREL_DIR, var_dir);
		strcat(QREL_DIR, "qrel/");
		struct stat st = {0};
		if (stat(QREL_DIR, &st) == -1) mkdir(QREL_DIR, 0705);
	}
	return QREL_DIR;
}

char *job_queue() {
	if (JOB_QUEUE_PATH == NULL) {
		char *var_dir = var_dir_path();
		JOB_QUEUE_PATH = (char*) malloc(strlen(var_dir) + 10);
		strcpy(JOB_QUEUE_PATH, var_dir);
		strcat(JOB_QUEUE_PATH, "job_queue");
		struct stat st = {0};
		if (stat(JOB_QUEUE_PATH, &st) == -1)
			mkfifo(JOB_QUEUE_PATH, 0707);
	}
	return JOB_QUEUE_PATH;
}

#endif
