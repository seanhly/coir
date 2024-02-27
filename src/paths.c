PathBuffer JOB_START_FILE_BUFFER = {0, 0};
PathBuffer JOB_END_FIFO_BUFFER = {0, 0};
PathBuffer QREL_FILE_BUFFER = {0, 0};

PathBuffer job_start_path_buffer() {
    if (JOB_START_FILE_BUFFER.length > 0)
        return JOB_START_FILE_BUFFER;
    char *the_job_start_dir = job_start_dir();
    int str_len = strlen(the_job_start_dir);
    JOB_START_FILE_BUFFER.buffer = (char*) malloc(str_len + 17);
    strcpy(JOB_START_FILE_BUFFER.buffer, the_job_start_dir);
    JOB_START_FILE_BUFFER.buffer[str_len + 16] = '\0';
    JOB_START_FILE_BUFFER.length = str_len;
    return JOB_START_FILE_BUFFER;
}

PathBuffer job_end_fifo_buffer() {
    if (JOB_END_FIFO_BUFFER.length > 0)
        return JOB_END_FIFO_BUFFER;
    char *the_job_end_dir = job_end_dir();
    int str_len = strlen(the_job_end_dir);
    JOB_END_FIFO_BUFFER.buffer = (char*) malloc(str_len + 17);
    strcpy(JOB_END_FIFO_BUFFER.buffer, the_job_end_dir);
    JOB_END_FIFO_BUFFER.buffer[str_len + 16] = '\0';
    JOB_END_FIFO_BUFFER.length = str_len;
    return JOB_END_FIFO_BUFFER;
}

PathBuffer qrel_path_buffer() {
	if (QREL_FILE_BUFFER.length > 0)
		return QREL_FILE_BUFFER;
	char *the_qrel_dir = qrel_dir();
	int str_len = strlen(the_qrel_dir);
	QREL_FILE_BUFFER.buffer = (char*) malloc(str_len + 19);
	strcpy(QREL_FILE_BUFFER.buffer, the_qrel_dir);
	QREL_FILE_BUFFER.buffer[str_len + 18] = '\0';
	QREL_FILE_BUFFER.length = str_len;
	return QREL_FILE_BUFFER;
}

char *qrel_file() {
	PathBuffer buff = qrel_path_buffer();
	char *qrel_file_path = buff.buffer;
	int qrel_file_length = buff.length;
	for (int i = 0; i < 2; i++) {
		char c = (char) ((CURRENT_QUERY >> (i * 4)) & 0xF);
		qrel_file_path[qrel_file_length + i] =
			c < 10 ? c + '0' : c - 10 + 'a';
	}
	qrel_file_path[qrel_file_length + 2] = '\0';
	struct stat st = {0};
	if (stat(qrel_file_path, &st) == -1) mkdir(qrel_file_path, 0700);
	qrel_file_path[qrel_file_length + 2] = '/';
	for (int i = 2; i < 4; i++) {
		char c = (char) ((CURRENT_QUERY >> (i * 4)) & 0xF);
		qrel_file_path[qrel_file_length + i + 1] =
			c < 10 ? c + '0' : c - 10 + 'a';
	}
	qrel_file_path[qrel_file_length + 5] = '\0';
	if (stat(qrel_file_path, &st) == -1) mkdir(qrel_file_path, 0700);
	qrel_file_path[qrel_file_length + 5] = '/';
	for (int i = 4; i < 16; i++) {
		char c = (char) ((CURRENT_QUERY >> (i * 4)) & 0xF);
		qrel_file_path[qrel_file_length + i + 2] =
			c < 10 ? c + '0' : c - 10 + 'a';
	}
	return qrel_file_path;
}
