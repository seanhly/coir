void err_exit(const char *msg) {
	printf("%s\n", msg);
	exit(EXIT_FAILURE);
}

void expected_qid_alone_err() {
	err_exit(
		"expected query ID alone on first input line, "
		"got multiple columns"
	);
}

void too_few_columns_err() {
	err_exit(
		"too few columns: "
		"input should be two-three columns, tab-separated"
	);
}

void too_many_columns_err() {
	err_exit(
		"too many columns: "
		"input should be two-three columns, tab-separated"
	);
}

void qid_unknown_err() {
	err_exit("qid results not loaded previously");
}

void error_reading_relevance() {
	err_exit("error reading relevance\n");
}

void error_reading_group() {
	err_exit("error reading group\n");
}

void duplicate_doc_id_err() {
    err_exit("duplicate doc_id");
}