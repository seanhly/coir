#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>

// compare session
int compare_session(const void *a, const void *b) {
	Session *sa = (Session*) a;
	Session *sb = (Session*) b;
	ui32 duplicates_a = sa->duplicates;
	ui32 duplicates_b = sb->duplicates;
	ui32 click_c_a = sa->click_c;
	ui32 click_c_b = sb->click_c;
	if (click_c_a < click_c_b) return -1;
	else if (click_c_a > click_c_b) return 1;
	else {
		Click *clicks_a = sa->clicks;
		Click *clicks_b = sb->clicks;
		for (ui32 i = 0; i < click_c_a; ++i) {
			ui16 pos_a = clicks_a[i].pos;
			ui16 pos_b = clicks_b[i].pos;
			if (pos_a < pos_b) return -1;
			else if (pos_a > pos_b) return 1;
			else {
				ui32 doc_a = clicks_a[i].doc;
				ui32 doc_b = clicks_b[i].doc;
				if (doc_a < doc_b)
					return -1;
				else if (doc_a > doc_b) return 1;
			}
		}
		return 0;
	}
}

void print_query(Query query) {
	fwrite(&query.session_c, sizeof(ui32), 1, stdout);
	for (ui32 i = 0; i < query.session_c; ++i) {
		fwrite(&query.sessions[i].sid, sizeof(ui32), 1, stdout);
		fwrite(&query.sessions[i].duplicates, sizeof(ui32), 1, stdout);
		ui16 prev_pos = query.sessions[i].clicks[0].pos;
		ui32 click_c = 1;
		bool valid_tail = true;
		while (valid_tail && click_c < query.sessions[i].click_c) {
			i32 pos = query.sessions[i].clicks[click_c].pos;
			i32 gap = pos - prev_pos;
			valid_tail = gap >= -128 && gap <= 127;
			if (valid_tail) {
				prev_pos = pos;
				click_c++;
			}
		}
		fwrite(&click_c, sizeof(ui16), 1, stdout);
		for (ui32 j = 0; j < click_c; ++j) {
			fwrite(&query.sessions[i].clicks[j].pos, sizeof(ui16), 1, stdout);
			fwrite(&query.sessions[i].clicks[j].doc, sizeof(ui32), 1, stdout);
		}
	}
}

void convert_click_log() {
	bool done = false;
	unsigned long line = 1;
	Query query;
	query.query = 0;
	query.session_c = 0;
	query.sessions = (Session*) malloc(sizeof(Session) * 400000);
	for (ui32 i = 0; i < 400000; ++i) {
		query.sessions[i].clicks = (Click*) malloc(sizeof(Click) * 3000);
	}
	ui32 qid;
	ui32 sid;
	ui16 pos;
	ui16 doc;
	ui32 prev_query = 0;
	// Read a character from the standard input
	new_line:
		char c = getchar();
		if (c == EOF) {
			done = true;
			goto end_of_line;
		}
		qid = 0;
		sid = 0;
		pos = 0;
		doc = 0;
		if (c < '0' || c > '9') {
			printf("at least one digit expected on line %lu\n", line);
			exit(1);
		}
		qid = qid * 10 + (c - '0');
		get_qid:
			c = getchar();
			if (c >= '0' && c <= '9') {
				qid = qid * 10 + (c - '0');
				goto get_qid;
			} else if (c != ',') {
				printf("comma expected after qid on line %lu\n", line);
				exit(1);
			}
			if (qid < prev_query || qid - prev_query > 1) {
				fprintf(stderr,
					"error: lines in the CSV should be sorted by query ID\n");
				exit(1);
			}
			prev_query = qid;
		c = getchar();
		if (c == EOF) {
			printf(
				"at least one digit expected after first comma on line %lu\n",
				line);
			exit(1);
		}
		if (c < '0' || c > '9') {
			printf(
				"at least one digit expected after first comma on line %lu\n",
				line);
			exit(1);
		}
		sid = sid * 10 + (c - '0');
		get_sid:
			c = getchar();
			if (c >= '0' && c <= '9') {
				sid = sid * 10 + (c - '0');
				goto get_sid;
			} else if (c != ',') {
				printf("comma expected after sid on line %lu\n", line);
				exit(1);
			}
		c = getchar();
		if (c == EOF) {
			printf(
				"at least one digit expected after second comma on line %lu\n",
				line);
			exit(1);
		}
		if (c < '0' || c > '9') {
			printf(
				"at least one digit expected after second comma on line %lu\n",
				line);
			exit(1);
		}
		pos = pos * 10 + (c - '0');
		get_pos:
			c = getchar();
			if (c >= '0' && c <= '9') {
				pos = pos * 10 + (c - '0');
				goto get_pos;
			} else if (c != ',') {
				printf("comma expected after pos on line %lu\n", line);
				exit(1);
			}
		c = getchar();
		if (c == EOF) {
			printf(
				"at least one digit expected after third comma on line %lu\n",
				line);
			exit(1);
		}
		if (c < '0' || c > '9') {
			printf(
				"at least one digit expected after third comma on line %lu\n",
				line);
			exit(1);
		}
		doc = doc * 10 + (c - '0');
		get_doc:
			c = getchar();
			if (c >= '0' && c <= '9') {
				doc = doc * 10 + (c - '0');
				goto get_doc;
			} else if (c == EOF) {
				done = true;
			} else if (c != '\n') {
				printf("newline expected after doc on line %lu\n", line);
				exit(1);
			}
	end_of_line:
		ui32 cc = query.session_c;
		if (qid == query.query) {
			if (sid == 0) {
				// First session for the query
				query.sessions[0].sid = sid;
				query.sessions[0].clicks[0].pos = pos;
				query.sessions[0].clicks[0].doc = doc;
				query.sessions[0].click_c = 1;
				query.sessions[0].sid = sid;
				query.sessions[0].duplicates = 1;
				query.session_c = 1;
			} else if (sid == query.sessions[cc - 1].sid) {
				// Same query and session
				ui16 click_c = query.sessions[cc - 1].click_c;
				query.sessions[cc - 1].clicks[click_c].pos = pos;
				query.sessions[cc - 1].clicks[click_c].doc = doc;
				query.sessions[cc - 1].click_c = click_c + 1;
			} else {
				// New session for the query
				cc = query.session_c++;
				query.sessions[cc].sid = sid;
				query.sessions[cc].clicks[0].pos = pos;
				query.sessions[cc].clicks[0].doc = doc;
				query.sessions[cc].click_c = 1;
				query.sessions[cc].duplicates = 1;
			}
		} else if (query.query == 0) {
			// First query
			query.query = qid;
			query.sessions[0].clicks[0].pos = pos;
			query.sessions[0].clicks[0].doc = doc;
			query.sessions[0].click_c = 1;
			query.sessions[0].sid = sid;
			query.sessions[0].duplicates = 1;
			query.session_c = 1;
		} else if (query.query != qid) {
			// A new query is being processed
			// Sort sessions
			qsort(
				query.sessions,
				query.session_c,
				sizeof(Session),
				compare_session);
			// Remove duplicate sessions via compare_session function
			ui32 i = 0;
			ui32 j = 1;
			while (j < query.session_c) {
				if (
					compare_session(&query.sessions[i], &query.sessions[j])
					== 0
				) {
					++query.sessions[i].duplicates;
					++j;
				} else {
					++i;
					if (i != j) {
						// Copy session from j to i
						query.sessions[i].click_c = query.sessions[j].click_c;
						query.sessions[i].duplicates =
							query.sessions[j].duplicates;
						query.sessions[i].sid = query.sessions[j].sid;
						// TODO can this be done faster using basic pointer move?
						for (ui32 k = 0; k < query.sessions[j].click_c; ++k) {
							query.sessions[i].clicks[k].pos =
								query.sessions[j].clicks[k].pos;
							query.sessions[i].clicks[k].doc =
								query.sessions[j].clicks[k].doc;
						}
					}
					++j;
				}
			}
			query.session_c = i + 1;
			print_query(query);
			query.query = qid;
			query.sessions[0].clicks[0].pos = pos;
			query.sessions[0].clicks[0].doc = doc;
			query.sessions[0].click_c = 1;
			query.sessions[0].sid = sid;
			query.sessions[0].duplicates = 1;
			query.session_c = 1;
		}
		if (!done) {
			++line;
			goto new_line;
		}
		// Free memory
		for (ui32 i = 0; i < 400000; ++i)
			free(query.sessions[i].clicks);
		free(query.sessions);
}
