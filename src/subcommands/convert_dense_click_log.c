#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <unordered_map>
#include <vector>

typedef struct {
	ui32 sid;
	ui16 rank;
	ui32 doc;
} SearchRankDoc;

typedef struct {
	ui32 duplicates;
	ui32 sid;
	ui16 *clicks;
	ui32 click_c;
	ui32 *ranked_docs;
	ui32 ranked_docs_c;
} ClickSession;

typedef struct {
	ui32 query;
	ClickSession *sessions;
	ui64 session_c;
} DenseQuerySession;

int compare_click_session(const void *a, const void *b) {
	ClickSession *sa = (ClickSession*) a;
	ClickSession *sb = (ClickSession*) b;
	ui32 click_c_a = sa->click_c;
	ui32 click_c_b = sb->click_c;
	ui32 ranked_docs_c = sa->ranked_docs_c;
	if (click_c_a < click_c_b) return -1;
	else if (click_c_a > click_c_b) return 1;
	else {
		ui16 *clicks_a = sa->clicks;
		ui16 *clicks_b = sb->clicks;
		for (ui32 i = 0; i < click_c_a; ++i) {
			ui16 pos_a = clicks_a[i];
			ui16 pos_b = clicks_b[i];
			if (pos_a < pos_b) return -1;
			else if (pos_a > pos_b) return 1;
		}
		for (ui32 i = 0; i < ranked_docs_c; ++i) {
			ui32 doc_a = sa->ranked_docs[i];
			ui32 doc_b = sb->ranked_docs[i];
			if (doc_a < doc_b) return -1;
			else if (doc_a > doc_b) return 1;
		}
		return 0;
	}
}

void convert_dense_click_log(
	char *click_log_path,
	char *rankings_path
) {
	ui8 file_opts = CONCRETE_RANK_DATA;
	fwrite(&file_opts, sizeof(ui8), 1, stdout);
	// Create a stream via zstdcat
	char *zstdcat_cmd = (char*) malloc(1000 * sizeof(char));
	sprintf(zstdcat_cmd, "/usr/bin/pv %s | /usr/bin/zstdcat", click_log_path);
	FILE *click_log_fp = popen(zstdcat_cmd, "r");
	// an unordered_map to store the click sessuences by search ID
	ClickSession *sessions = (ClickSession*)
		malloc(100000000 * sizeof(ClickSession));
	ui64 session_c = 0;
	ui32 sid;
	ui16 rank;
	char c;
	ui16 *click_ranks = (ui16*) malloc(100000000 * sizeof(ui16));
	ui16 *click_rank_ptr = click_ranks;
	ui32 prev_sid = 0;
	ui16 *sess_start = click_ranks;
	while ((c = fgetc(click_log_fp)) != EOF) {
		sid = 0;
		while (c != ',') {
			sid = sid * 10 + (c - '0');
			c = fgetc(click_log_fp);
		}
		rank = 0;
		c = fgetc(click_log_fp);
		while (c >= '0' && c <= '9') {
			rank = rank * 10 + (c - '0');
			c = fgetc(click_log_fp);
		}
		if (prev_sid != sid) {
			if (click_rank_ptr > sess_start) {
				sessions[session_c++] = {
					1,
					prev_sid,
					sess_start,
					(ui16) (click_rank_ptr - sess_start)
				};
			}
			sess_start = click_rank_ptr;
			prev_sid = sid;
		}
		*(click_rank_ptr++) = rank;
	}
	if (click_rank_ptr > sess_start)
		sessions[session_c++] = {
			1,
			prev_sid,
			sess_start,
			(ui16) (click_rank_ptr - sess_start)
		};
	ui32 *rank_docs = (ui32*) malloc(200000000 * sizeof(ui32));
	ui32 *rank_doc_ptr = rank_docs;
	ui32 qid;
	ui32 doc = 0;
	pclose(click_log_fp);
	sprintf(zstdcat_cmd, "/usr/bin/pv %s | /usr/bin/zstdcat", rankings_path);
	FILE *rankings_fp = popen(zstdcat_cmd, "r");
	ui32 prev_query = 0;
	prev_sid = 0;
	DenseQuerySession session;
	session.query = 0;
	session.session_c = 0;
	ClickSession *curr_click_session = sessions;
	while ((c = fgetc(rankings_fp)) != EOF) {
		qid = 0;
		while (c != ',') {
			qid = qid * 10 + (c - '0');
			c = fgetc(rankings_fp);
		}
		if (qid != prev_query + 1) {
			fprintf(stderr, "error: queries in the CSV should be sorted by query ID\n");
			exit(1);
		}
		prev_query = qid;
		sid = 0;
		c = fgetc(rankings_fp);
		while (c != ',') {
			sid = sid * 10 + (c - '0');
			c = fgetc(rankings_fp);
		}
		if (qid != session.query) {
			if (session.session_c > 0) {
				fprintf(stderr, "Query %d\n", session.query);
				qsort(
					session.sessions,
					session.session_c,
					sizeof(ClickSession),
					compare_click_session
				);
				ui32 i = 0;
				ui32 j = 1;
				while (j < session.session_c) {
					if (
						compare_click_session(
							&session.sessions[i],
							&session.sessions[j]
						)
						== 0
					) {
						++session.sessions[i].duplicates;
						++j;
					} else {
						++i;
						if (i != j)
							session.sessions[i] = session.sessions[j];
						++j;
					}
				}
				session.session_c = i + 1;
				fwrite(&session.session_c, sizeof(ui32), 1, stdout);
				fprintf(stderr, "Session count: %lu\n", session.session_c);
				// Iterate over click sessions...
				for (ui64 i = 0; i < session.session_c; i++) {
					ClickSession *click_session =
						&session.sessions[i];
					fwrite(&click_session->duplicates,
						sizeof(ui32), 1, stdout);
					fprintf(stderr, "\tDuplicates: %d\n", click_session->duplicates);
					fprintf(stderr, "\tClicks: %d\n", click_session->click_c);
					ui16 click_c = 1;
					ui16 prev_rank = click_session->clicks[0];
					ui32 max_click_c = click_session->click_c;
					if (max_click_c > (ui16) 0xffff)
						max_click_c = (ui16) 0xffff;
					bool valid_tail = true;
					while (click_c < max_click_c && valid_tail) {
						ui16 rank = click_session->clicks[click_c];
						i32 gap = (ui32) rank - (ui32) prev_rank;
						valid_tail = gap >= -128 && gap <= 127;
						if (valid_tail) {
							click_c++;
							prev_rank = rank;
						}
					}
					fwrite(&click_c, sizeof(ui16), 1, stdout);
					// Iterate over clicks...
					ui16 max_rank = 0;
					for (ui16 j = 0; j < click_c; j++) {
						ui16 rank = click_session->clicks[j];
						if (rank > max_rank) max_rank = rank;
						fprintf(stderr, "\t\t>> CLICKED << %d\n", click_session->clicks[j]);
						if (j == 0) fwrite(&rank, sizeof(ui16), 1, stdout);
						else {
							i8 gap = (ui32) rank - (ui32) prev_rank;
							fwrite(&gap, sizeof(i8), 1, stdout);
						}
						prev_rank = rank;
					}
					fprintf(stderr, "\tRanked docs count: %d\n", click_session->ranked_docs_c);
					// Iterate over ranked docs...
					fprintf(stderr, "\t");
					for (ui32 j = 0; j < max_rank; j++) {
						ui32 doc = click_session->ranked_docs[j];
						fprintf(stderr, "doc%u@%u ", doc, j + 1);
						fwrite(&doc, sizeof(ui32), 1, stdout);
					}
					fprintf(stderr, "\n");
				}
			}
			session.query = qid;
			session.session_c = 1;
			curr_click_session->ranked_docs_c = 0;
			curr_click_session->ranked_docs = rank_doc_ptr;
			session.sessions = curr_click_session++;
		} else if (
			session.session_c > 0 &&
			sid != session.sessions[
			session.session_c - 1].sid
		) {
			session.session_c++;
			curr_click_session->ranked_docs_c = 0;
			curr_click_session->ranked_docs = rank_doc_ptr;
			curr_click_session++;
		}
		rank = 0;
		c = fgetc(rankings_fp);
		while (c >= '0' && c <= '9') {
			rank = rank * 10 + (c - '0');
			c = fgetc(rankings_fp);
		}
		doc = 0;
		c = fgetc(rankings_fp);
		while (c >= '0' && c <= '9') {
			doc = doc * 10 + (c - '0');
			c = fgetc(rankings_fp);
		}
		*(rank_doc_ptr++) = doc;
		ClickSession *click_session = &session.sessions[
			session.session_c - 1];
		click_session->ranked_docs_c++;
	}
	free(zstdcat_cmd);
}
