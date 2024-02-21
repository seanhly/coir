#define MIN(a, b) ((a) < (b) ? (a) : (b))
#define MAX(a, b) ((a) > (b) ? (a) : (b))

PathBuffer JOB_START_FILE_BUFFER = {0, 0};
PathBuffer JOB_END_FIFO_BUFFER = {0, 0};
PathBuffer QREL_FILE_BUFFER = {0, 0};
PathBuffer ID_STR_MAP_PATH = {0, 0};
Candidate *DOC_RELEVANCE_UPDATE_BUFFER = NULL;
ui8 *ATTENTION_BUFFER = NULL;
Candidate **RANKING_BUFFER = NULL;
UpdateAction *UPDATE_ACTION_BUFFER = NULL;

// Find the index of the right-most element that is above or equal to a value
long lowest_above_or_equals(Candidate *arr, ui64 len, f64 v) {
	if (len == 0) return -1;
    long lo = 0;
    long hi = len - 1;
    if (lo == hi) {
    	if (arr[lo].relevance >= v) return lo;
		else return -1;
	}
	while (1) {
    	f64 highest_value = arr[lo].relevance;
    	f64 lowest_value = arr[hi].relevance;
    	f64 v_range = highest_value - lowest_value;
    	f64 i_range = hi - lo;
    	if (v < lowest_value) {
    		for (int i = hi; i >= 0; --i) 
				if (arr[i].relevance != lowest_value) return i + 1;
    		return 0;
    	}
    	if (v > highest_value) return -1;
    	f64 v_offset = v - lowest_value;
		f64 v_proportion = v_offset / v_range;
    	long i_mid = hi - floor(i_range * v_proportion);
		f64 v_mid = arr[i_mid].relevance;
		if (v_mid == v) {
			for (int i = i_mid; i >= 0; --i)
				if (arr[i].relevance != v) return i + 1;
			return 0;
		} else if (v_mid < v) hi = i_mid - 1;
		else lo = i_mid;
	}
}

// Find the index of the left-most element that is below a value
long first_below(Candidate *arr, ui64 len, f64 v) {
	return -1;
}

void err_exit(const char *msg) {
	printf("%s\n", msg);
	exit(EXIT_FAILURE);
}

FILE* safe_file_append(const char *path) {
	FILE *fp = fopen(path, "ab");
	if (fp == NULL) {
		char *msg = (char*) malloc(strlen(path) + 200);
		strcpy(msg, "Failed to open file for appending: ");
		strcat(msg, path);
		err_exit(msg);
	}
	return fp;
}

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

void safe_file_remove(const char *path) {
	if (remove(path) != 0) err_exit("remove");
}

FILE* init_job_file(char *job_file, Job job_type) {
	FILE *job_fp = safe_file_append(job_file);
	ui8 job_type_char = (ui8) job_type;
	fwrite(&job_type_char, 1, 1, job_fp);
	return job_fp;
}

PathBuffer id_str_map_buffer() {
	if (ID_STR_MAP_PATH.length > 0) return ID_STR_MAP_PATH;
	char *cache_dir = cache_dir_path();
	int str_len = strlen(cache_dir) + 11;
	ID_STR_MAP_PATH.buffer = (char*) malloc(str_len + 19);
	strcpy(ID_STR_MAP_PATH.buffer, cache_dir);
	strcat(ID_STR_MAP_PATH.buffer, "id_str_map/");
	struct stat st = {0};
	if (stat(ID_STR_MAP_PATH.buffer, &st) == -1)
		mkdir(ID_STR_MAP_PATH.buffer, 0700);
	ID_STR_MAP_PATH.buffer[str_len + 18] = '\0';
	ID_STR_MAP_PATH.length = str_len;
	return ID_STR_MAP_PATH;
}

ui8 *attention_buffer() {
	if (ATTENTION_BUFFER != NULL) return ATTENTION_BUFFER;
	// Give some extra space for in-place calculations
	ATTENTION_BUFFER = (ui8*) malloc(PAGE_LENGTH * sizeof(f64));
	return ATTENTION_BUFFER;
}

UpdateAction* update_action_buffer() {
	if (UPDATE_ACTION_BUFFER != NULL) return UPDATE_ACTION_BUFFER;
	UPDATE_ACTION_BUFFER = (UpdateAction*) malloc(PAGE_LENGTH * 2 * sizeof(UpdateAction));
	return UPDATE_ACTION_BUFFER;
}

Candidate **ranking_buffer() {
	if (RANKING_BUFFER != NULL) return RANKING_BUFFER;
	RANKING_BUFFER = (Candidate**) malloc(PAGE_LENGTH * sizeof(Candidate*));
	return RANKING_BUFFER;
}

char *get_orig_doc_id_path(unsigned long doc_id) {
	PathBuffer id_str_map_buff = id_str_map_buffer();
	char *id_str_map_path = id_str_map_buff.buffer;
	int id_str_map_length = id_str_map_buff.length;
	for (int i = 0; i < 2; i++) {
		char c = (char) ((doc_id >> (i * 4)) & 0xF);
		id_str_map_path[id_str_map_length + i] =
			c < 10 ? c + '0' : c - 10 + 'a';
	}
	id_str_map_path[id_str_map_length + 2] = '\0';
	struct stat st = {0};
	if (stat(id_str_map_path, &st) == -1) mkdir(id_str_map_path, 0700);
	id_str_map_path[id_str_map_length + 2] = '/';
	for (int i = 2; i < 4; i++) {
		char c = (char) ((doc_id >> (i * 4)) & 0xF);
		id_str_map_path[id_str_map_length + i + 1] =
			c < 10 ? c + '0' : c - 10 + 'a';
	}
	id_str_map_path[id_str_map_length + 5] = '\0';
	if (stat(id_str_map_path, &st) == -1) mkdir(id_str_map_path, 0700);
	id_str_map_path[id_str_map_length + 5] = '/';
	for (int i = 4; i < 16; i++) {
		char c = (char) ((doc_id >> (i * 4)) & 0xF);
		id_str_map_path[id_str_map_length + i + 2] =
			c < 10 ? c + '0' : c - 10 + 'a';
	}
	return id_str_map_path;
}

unsigned long gen_id(char *data) {
	OpenSSL_add_all_algorithms();
	ERR_load_crypto_strings();
	EVP_MD_CTX *mdctx;
	if((mdctx = EVP_MD_CTX_new()) == NULL) handleErrors();
	if(1 != EVP_DigestInit_ex(mdctx, EVP_sha256(), NULL)) handleErrors();
	if(1 != EVP_DigestUpdate(mdctx, data, strlen(data))) handleErrors();
	ui8 hash[EVP_MAX_MD_SIZE];
	unsigned int hash_len;
	if(1 != EVP_DigestFinal_ex(mdctx, hash, &hash_len)) handleErrors();
	EVP_MD_CTX_free(mdctx);
	EVP_cleanup();
	ERR_free_strings();
	unsigned long result = 0;
	for (int i = 0; i < sizeof(result) && i < (int)hash_len; i++)
  	  result = (result << 8) | hash[i];
	char *id_str_map_path = get_orig_doc_id_path(result);
	FILE *id_str_map_fp = fopen(id_str_map_path, "r");
	if (id_str_map_fp == NULL) {
		id_str_map_fp = fopen(id_str_map_path, "w");
		while (*data != '\0') fputc(*data++, id_str_map_fp);
	}
	fclose(id_str_map_fp);
	return result;
}

Candidate *doc_relevance_update_buffer() {
	if (DOC_RELEVANCE_UPDATE_BUFFER != NULL)
		return DOC_RELEVANCE_UPDATE_BUFFER;
	DOC_RELEVANCE_UPDATE_BUFFER = (Candidate*)
		malloc(MAX_RESULTS_PER_RERANK * sizeof(Candidate));
	return DOC_RELEVANCE_UPDATE_BUFFER;
}

PathBuffer job_start_file_buffer() {
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

PathBuffer qrel_file_buffer() {
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

char *qrel_file(unsigned long qid) {
	PathBuffer buff = qrel_file_buffer();
	char *qrel_file_path = buff.buffer;
	int qrel_file_length = buff.length;
	for (int i = 0; i < 2; i++) {
		char c = (char) ((qid >> (i * 4)) & 0xF);
		qrel_file_path[qrel_file_length + i] =
			c < 10 ? c + '0' : c - 10 + 'a';
	}
	qrel_file_path[qrel_file_length + 2] = '\0';
	struct stat st = {0};
	if (stat(qrel_file_path, &st) == -1) mkdir(qrel_file_path, 0700);
	qrel_file_path[qrel_file_length + 2] = '/';
	for (int i = 2; i < 4; i++) {
		char c = (char) ((qid >> (i * 4)) & 0xF);
		qrel_file_path[qrel_file_length + i + 1] =
			c < 10 ? c + '0' : c - 10 + 'a';
	}
	qrel_file_path[qrel_file_length + 5] = '\0';
	if (stat(qrel_file_path, &st) == -1) mkdir(qrel_file_path, 0700);
	qrel_file_path[qrel_file_length + 5] = '/';
	for (int i = 4; i < 16; i++) {
		char c = (char) ((qid >> (i * 4)) & 0xF);
		qrel_file_path[qrel_file_length + i + 2] =
			c < 10 ? c + '0' : c - 10 + 'a';
	}
	return qrel_file_path;
}

void get_orig_doc_id(unsigned long doc_id, char *buffer) {
	char *id_str_map_path = get_orig_doc_id_path(doc_id);
	FILE *id_str_map_fp = safe_file_read(id_str_map_path);
	int i = 0;
	char c;
	while ((c = fgetc(id_str_map_fp)) != EOF) buffer[i++] = c;
	fclose(id_str_map_fp);
	buffer[i] = '\0';
}

unsigned long t() {
	struct timespec ts;
	unsigned long currentTimeNano;
	if (clock_gettime(CLOCK_REALTIME, &ts) == -1) {
		perror("clock_gettime");
		exit(EXIT_FAILURE);
	}
	currentTimeNano = (long)ts.tv_sec * 1000000000LL + ts.tv_nsec;
	return currentTimeNano;
}

void print_rank_candidate_lists(Context ctx) {
	for (ui64 i = 0; i < ctx.rank_contexts_c; ++i) {
		printf("%lu. ", i);
		for (ui64 j = 0; j < ctx.rank_contexts[i].candidates_c; ++j) {
			printf(
				"%0.2lf ",
				ctx.rank_contexts[i].candidates_ptr[j]->relevance
			);
		}
		printf("\n");
	}
}

void print_rc(RankContext rc) {
	for (ui64 i = 0; i < rc.candidates_c; ++i)
		printf("  %lf", rc.candidates_ptr[i]->relevance);
	printf("\n");
}

void print_candidate(Candidate c) {
	printf(
		"doc_id: %lu, relevance: %f, exposure: %lu, masked: %d\n",
		c.doc_id, c.relevance, c.exposure, c.masked
	);
}

void safe_write_item(void *content, size_t size, FILE *fp) {
	if (fwrite(content, size, 1, fp) != 1)
		err_exit("safe_write error");
}

void safe_read_item(void *content, size_t size, FILE *fp) {
	if (fread(content, size, 1, fp) != 1)
		err_exit("safe_read error");
}

// Create a comparator for the Candidate struct
// used in the std::sort function
bool compare_candidate(Candidate x, Candidate y) {
	bool result =
		x.relevance * (y.exposure + REALLY_SMALL_VALUE)
		>
		y.relevance * (x.exposure + REALLY_SMALL_VALUE) ||
		(
			x.relevance * (y.exposure + REALLY_SMALL_VALUE)
			==
			y.relevance * (x.exposure + REALLY_SMALL_VALUE)
			&&
			x.doc_id > y.doc_id
		);
	return result;
}

bool compare_candidate_by_relevance(Candidate x, Candidate y) {
	return 
		x.relevance > y.relevance ||
		(x.relevance == y.relevance && x.doc_id > y.doc_id);
}

bool compare_candidate_ptr(Candidate *x, Candidate *y) {
	return compare_candidate(*x, *y);
}

void compact_print_float(f64 flt) {
	if (flt == 0) {
		printf("0");
		return;
	}
	if (flt < 0) {
		printf("-");
		flt = -flt;
	}
	int flt_as_int = (int) flt;
	flt -= flt_as_int;
	int reverse_digits = 0;
	while (flt_as_int > 0) {
		reverse_digits = reverse_digits * 10 + flt_as_int % 10;
		flt_as_int /= 10;
	}
	while (reverse_digits > 0) {
		printf("%d", reverse_digits % 10);
		reverse_digits /= 10;
	}
	if (flt > 0.0000000001) {
		printf(".");
		while (flt > 0.0000000001) {
			flt *= 10;
			int candidate = (int) flt;
			flt -= (int) flt;
			if (1 - flt < 0.0000000001) {
				printf("%d", candidate + 1);
				break;
			} else {
				printf("%d", candidate);
			}
		}
	}
}
