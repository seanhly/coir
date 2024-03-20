#ifndef IO_C
#define IO_C

#include "utils.c"
#include "fp.c"
#include <stdio.h>

void safe_file_remove(const char *path) {
    if (remove(path) != 0) {
    	err_exit(path);
	}
}

qid qid_from_client() {
    qid query;
    if (fread(&query, sizeof(qid), 1, read_job_file()) != 1)
        err_exit("error reading query\n");
    return query;
}

f64 quality_threshold_from_client() {
    f64 quality_threshold;
    if (fread(&quality_threshold, sizeof(f64), 1, read_job_file()) != 1)
        err_exit("error reading quality_threshold\n");
    return quality_threshold;
}

ui64 repetitions_from_client() {
    ui64 repetitions;
    if (fread(&repetitions, sizeof(ui64), 1, read_job_file()) != 1)
        err_exit("error reading repetitions\n");
    return repetitions;
}

f64 unfairness_from_daemon() {
    f64 unfairness;
    if (fread(&unfairness, sizeof(f64), 1, read_job_end_file()) != 1)
        err_exit("error reading unfairness\n");
    return unfairness;
}

f64 relevance_from_daemon() {
    f64 relevance;
    if (fread(&relevance, sizeof(f64), 1, read_job_end_file()) != 1)
        err_exit("error reading unfairness\n");
    return relevance;
}

bool doc_from_daemon(docid &doc) {
    return fread(&doc, sizeof(docid), 1, read_job_end_file()) == 1;
}

Job job_type_from_client() {
	ui8 job_ui8;
    if (fread(&job_ui8, 1, 1, read_job_file()) != 1)
        err_exit("error reading job\n");
    return (Job) job_ui8;
}

void ui64_to_client(ui64 value) {
    if (fwrite(&value, sizeof(ui64), 1, write_job_end_file()) != 1)
        err_exit("error writing ui64\n");
}

bool ui64_from_daemon(ui64 &value) {
    return fread(&value, sizeof(ui64), 1, read_job_end_file()) == 1;
}

void doc_to_client(docid doc) {
    if (fwrite(&doc, sizeof(docid), 1, write_job_end_file()) != 1)
        err_exit("error writing docid\n");
}

void relevance_to_client(f64 value) {
    if (fwrite(&value, sizeof(f64), 1, write_job_end_file()) != 1)
        err_exit("error writing relevance\n");
}

void unfairness_to_client(f64 value) {
    if (fwrite(&value, sizeof(f64), 1, write_job_end_file()) != 1)
        err_exit("error writing unfairness\n");
}

void quality_threshold_to_daemon(f64 threshold) {
    if (fwrite(&threshold, sizeof(f64), 1, write_job_file()) != 1)
        err_exit("error writing quality_threshold\n");
}

void qid_to_daemon(qid query) {
    if (fwrite(&query, sizeof(qid), 1, write_job_file()) != 1)
        err_exit("error writing qid\n");
}

void doc_to_daemon(docid doc) {
    if (fwrite(&doc, sizeof(docid), 1, write_job_file()) != 1)
        err_exit("error writing docid\n");
}

void repetitions_to_daemon(ui64 reps) {
    if (fwrite(&reps, sizeof(ui64), 1, write_job_file()) != 1)
        err_exit("error writing repetitions\n");
}

void relevance_to_daemon(f64 relevance) {
    if (fwrite(&relevance, sizeof(f64), 1, write_job_file()) != 1)
        err_exit("error writing relevance\n");
}

void group_to_daemon(ui8 group) {
    if (fwrite(&group, sizeof(ui8), 1, write_job_file()) != 1)
        err_exit("error writing group\n");
}

void cancel_job() {
	printf("Canceling job.\n");
    fclose(JOB_FILE);
    safe_file_remove(job_start_path_buffer().buffer);
}

bool doc_from_client(docid &doc) {
    return fread(&doc, sizeof(docid), 1, read_job_file()) == 1;
}

f64 relevance_from_client() {
    f64 relevance;
    if (fread(&relevance, sizeof(f64), 1, read_job_file()) != 1)
        err_exit("error reading relevance\n");
    return relevance;
}

ui8 group_from_client() {
    ui8 group;
    if (fread(&group, sizeof(ui8), 1, read_job_file()) != 1)
        error_reading_group();
    return group;
}

qid qid_from_stdin(char &c) {
    ui64 offset = 0;
    char *focus_buffer = get_focus_buffer();
    while ((c = getchar()) != EOF) {
        if (c == '\t') break;
        else if (c == '\n') break;
        focus_buffer[offset++] = c;
    }
    focus_buffer[offset] = '\0';
    qid query = gen_id();
    return query;
}

void doc_to_qrel(docid doc) {
    if (fwrite(&doc, sizeof(docid), 1, write_qrel_file()) != 1)
        err_exit("error writing docid\n");
}

void relevance_to_qrel(f64 relevance) {
	if (fwrite(&relevance, sizeof(f64), 1, write_qrel_file()) != 1)
		err_exit("error writing relevance\n");
}

void group_to_qrel(ui8 group) {
	if (fwrite(&group, sizeof(ui8), 1, write_qrel_file()) != 1)
		err_exit("error writing group\n");
}

void exposure_to_qrel(f64 exposure) {
	if (fwrite(&exposure, sizeof(f64), 1, write_qrel_file()) != 1)
		err_exit("error writing exposure\n");
}

void candidate_to_qrel(Candidate candidate) {
	doc_to_qrel(candidate.doc);
	relevance_to_qrel(candidate.relevance);
	exposure_to_qrel(candidate.exposure);
	group_to_qrel(DOC_GROUPS[candidate.doc]);
}

void len_to_qrel(ui64 len) {
	if (fwrite(&len, sizeof(ui64), 1, write_qrel_file()) != 1)
		err_exit("error writing len\n");
}

ui64 len_from_qrel() {
	ui64 len;
	if (fread(&len, sizeof(ui64), 1, read_qrel_file()) != 1)
		err_exit("error reading len\n");
	return len;
}

docid doc_from_qrel() {
	docid doc;
	if (fread(&doc, sizeof(docid), 1, read_qrel_file()) != 1)
		err_exit("error reading docid\n");
	return doc;
}

f64 relevance_from_qrel() {
	f64 relevance;
	if (fread(&relevance, sizeof(f64), 1, read_qrel_file()) != 1)
		err_exit("error reading relevance\n");
	return relevance;
}

ui8 group_from_qrel() {
	ui8 group;
	if (fread(&group, sizeof(ui8), 1, read_qrel_file()) != 1)
		err_exit("error reading group\n");
	return group;
}

f64 exposure_from_qrel() {
	f64 exposure;
	if (fread(&exposure, sizeof(f64), 1, read_qrel_file()) != 1)
		err_exit("error reading exposure\n");
	return exposure;
}

void safe_read(void *ptr, size_t size, FILE *stream) {
	if (fread(ptr, size, 1, stream) != 1) {
		fprintf(stderr, "Error reading from file\n");
		exit(1);
	}
}

#endif
