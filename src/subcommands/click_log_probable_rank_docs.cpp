#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <unordered_map>

typedef struct {
	ui32 first;
	ui32 second;
} UI32Tuple;

int compare_ui32_tuple(const void *a, const void *b) {
	UI32Tuple *sa = (UI32Tuple*) a;
	UI32Tuple *sb = (UI32Tuple*) b;
	ui32 first_a = sa->first;
	ui32 first_b = sb->first;
	if (first_a < first_b) return -1;
	else if (first_a > first_b) return 1;
	ui32 second_a = sa->second;
	ui32 second_b = sb->second;
	if (second_a < second_b) return -1;
	else if (second_a > second_b) return 1;
	return 0;
}

void click_log_probable_rank_docs() {
	ui32 qid;
	ui32 sid;
	ui32 session_c;
	ui32 duplicates;
	ui16 click_c;
	ui16 rank;
	ui32 url;
	ui16 highest_rank;
	ui16 rank_c;
	std::unordered_map<ui32, ui32> rank_doc_mass_map[500];
	UI32Tuple *rank_doc_mass_buff =
		(UI32Tuple*) malloc(sizeof(UI32Tuple) * 100000);
	while (fread(&qid, sizeof(ui32), 1, stdin) == 1) {
		for (ui16 i = 0; i < 500; ++i) rank_doc_mass_map[i].clear();
		safe_read(&session_c, sizeof(ui32), stdin);
		highest_rank = 1;
		rank_c = 0;
		for (ui32 i = 0; i < session_c; ++i) {
			safe_read(&sid, sizeof(ui32), stdin);
			safe_read(&duplicates, sizeof(ui32), stdin);
			safe_read(&click_c, sizeof(ui16), stdin);
			for (ui32 j = 0; j < click_c; ++j) {
				safe_read(&rank, sizeof(ui16), stdin);
				if (rank > highest_rank) highest_rank = rank;
				safe_read(&url, sizeof(ui32), stdin);
				if (rank_doc_mass_map[rank - 1].empty()) ++rank_c;
				if (
					rank_doc_mass_map[rank - 1].find(url)
					== rank_doc_mass_map[rank - 1].end()
				)
					rank_doc_mass_map[rank - 1][url] = duplicates;
				else rank_doc_mass_map[rank - 1][url] += duplicates;
			}
		}
		fwrite(&qid, sizeof(ui32), 1, stdout);
		fprintf(stderr, "QID: %u\n", qid);
		fwrite(&rank_c, sizeof(ui16), 1, stdout);
		fprintf(stderr, "Total ranks: %u\n", rank_c);
		for (ui16 rank = 1; rank <= highest_rank; ++rank) {
			ui32 dist_len = 0;
			ui32 total_mass = 0;
			for (
				auto it = rank_doc_mass_map[rank - 1].begin();
				it != rank_doc_mass_map[rank - 1].end();
				++it
			) {
				total_mass += it->second;
				rank_doc_mass_buff[dist_len++] = {it->second, it->first};
			}
			if (total_mass == 0) continue;
			fprintf(stderr, "\tRank: %u ", rank);
			fprintf(stderr, "(%u)\n", total_mass);
			qsort(
				rank_doc_mass_buff,
				dist_len,
				sizeof(UI32Tuple),
				compare_ui32_tuple
			);
			fwrite(&rank, sizeof(ui16), 1, stdout);
			fwrite(&total_mass, sizeof(ui32), 1, stdout);
			while (dist_len-- != 0) {
				ui32 doc_mass = rank_doc_mass_buff[dist_len].first;
				ui32 doc = rank_doc_mass_buff[dist_len].second;
				fwrite(&doc, sizeof(ui32), 1, stdout);
				fwrite(&doc_mass, sizeof(ui32), 1, stdout);
				fprintf(stderr, "\t\t%u : %u\n", doc, doc_mass);
			}
		}
	}
}
