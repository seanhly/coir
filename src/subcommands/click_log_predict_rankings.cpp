#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <unordered_map>
#include <unordered_set>

int compare_ui32_tuple_desc(const void *a, const void *b) {
	UI32Tuple *sa = (UI32Tuple*) a;
	UI32Tuple *sb = (UI32Tuple*) b;
	ui32 first_a = sa->first;
	ui32 first_b = sb->first;
	if (first_a < first_b) return 1;
	else if (first_a > first_b) return -1;
	ui32 second_a = sa->second;
	ui32 second_b = sb->second;
	if (second_a < second_b) return 1;
	else if (second_a > second_b) return -1;
	return 0;
}


void click_log_predict_rankings() {
	ui8 file_opts = PROBABILISTIC_RANK_DATA;
	fwrite(&file_opts, sizeof(ui8), 1, stdout);
	ui32 qid;
	ui32 sid;
	ui32 session_c;
	ui32 duplicates;
	ui16 click_c;
	ui16 rank;
	ui32 doc;
	ui16 highest_rank;
	ui16 rank_c;
	std::unordered_map<ui32, ui32> *rank_doc_mass_map =
		(std::unordered_map<ui32, ui32>*)
			malloc(sizeof(std::unordered_map<ui32, ui32>) * MAX_RANK);
	for (ui16 i = 0; i < MAX_RANK; ++i)
		rank_doc_mass_map[i] = std::unordered_map<ui32, ui32>();
	bool *masked_ranks = (bool*) malloc(sizeof(bool) * MAX_RANK);
	std::unordered_set<ui32> masked_docs;
	ui32 buff_size = 1000000;
	char *buffer = (char*) malloc(buff_size);
	FILE *memstream = fmemopen(buffer, buff_size, "rb+");
	UI32Tuple *ranking_buff =
		(UI32Tuple*) malloc(sizeof(UI32Tuple) * MAX_RANK);
	UI32Tuple *schrodinger_buff =
		(UI32Tuple*) malloc(sizeof(UI32Tuple) * 100000);
	while (fread(&session_c, sizeof(ui32), 1, stdin) == 1) {
		for (ui16 i = 0; i < MAX_RANK; ++i) rank_doc_mass_map[i].clear();
		rewind(memstream);
		fwrite(&session_c, sizeof(ui32), 1, memstream);
		rank_c = 0;
		for (ui32 i = 0; i < session_c; ++i) {
			safe_read(&sid, sizeof(ui32), stdin);
			fwrite(&sid, sizeof(ui32), 1, memstream);
			safe_read(&duplicates, sizeof(ui32), stdin);
			fwrite(&duplicates, sizeof(ui32), 1, memstream);
			safe_read(&click_c, sizeof(ui16), stdin);
			fwrite(&click_c, sizeof(ui16), 1, memstream);
			for (ui32 j = 0; j < click_c; ++j) {
				safe_read(&rank, sizeof(ui16), stdin);
				fwrite(&rank, sizeof(ui16), 1, memstream);
				safe_read(&doc, sizeof(ui32), stdin);
				fwrite(&doc, sizeof(ui32), 1, memstream);
				if (rank_doc_mass_map[rank - 1].empty()) ++rank_c;
				if (
					rank_doc_mass_map[rank - 1].find(doc)
					== rank_doc_mass_map[rank - 1].end()
				)
					rank_doc_mass_map[rank - 1][doc] = duplicates;
				else rank_doc_mass_map[rank - 1][doc] += duplicates;
			}
		}
		rewind(memstream);
		fwrite(&session_c, sizeof(ui32), 1, stdout);
		safe_read(&session_c, sizeof(ui32), memstream);
		rank_c = 0;
		for (ui32 i = 0; i < session_c; ++i) {
			highest_rank = 1;
			ui16 ranking_buff_c = 0;
			for (ui16 j = 0; j < MAX_RANK; ++j) masked_ranks[j] = false;
			masked_docs.clear();
			safe_read(&sid, sizeof(ui32), memstream);
			safe_read(&duplicates, sizeof(ui32), memstream);
			fwrite(&duplicates, sizeof(ui32), 1, stdout);
			safe_read(&click_c, sizeof(ui16), memstream);
			fwrite(&click_c, sizeof(ui16), 1, stdout);
			ui16 prev_rank;
			for (ui32 j = 0; j < click_c; ++j) {
				safe_read(&rank, sizeof(ui16), memstream);
				if (j == 0) fwrite(&rank, sizeof(ui16), 1, stdout);
				else {
					i8 gap = (i8) (((i16) rank) - ((i16) prev_rank));
					fwrite(&gap, sizeof(i8), 1, stdout);
				}
				prev_rank = rank;
				if (rank > highest_rank) highest_rank = rank;
				safe_read(&doc, sizeof(ui32), memstream);
				masked_docs.insert(doc);
				if (!masked_ranks[rank - 1]) {
					ranking_buff[ranking_buff_c++] = {rank, doc};
					masked_ranks[rank - 1] = true;
				}
			}
			ui16 last_rank_removed = highest_rank;
			ui16 rank = 1;
			ui16 max_rank = 0;
			do {
				if (!masked_ranks[rank - 1]) {
					auto map = rank_doc_mass_map[rank - 1];
					ui64 map_size = map.size();
					ui32 unmasked_doc_c = 0;
					ui32 doc;
					for (auto it = map.begin(); it != map.end(); ++it) {
						b unmasked =
							masked_docs.find(it->first) == masked_docs.end();
						unmasked_doc_c += unmasked;
						if (unmasked) doc = it->first;
					}
					if (unmasked_doc_c == 1) {
						masked_ranks[rank - 1] = true;
						masked_docs.insert(doc);
						last_rank_removed = rank;
						ranking_buff[ranking_buff_c++] = {rank, doc};
					}
				}
				rank = rank == highest_rank ? 1 : rank + 1;
			} while (rank != last_rank_removed);
			qsort(
				ranking_buff,
				ranking_buff_c,
				sizeof(UI32Tuple),
				compare_ui32_tuple
			);
			ui16 last_rank = 0;
			for (ui16 j = 0; j < ranking_buff_c; ++j) {
				UI32Tuple item = ranking_buff[j];
				ui16 rank = item.first;
				while (++last_rank != rank) {
					std::unordered_map<ui32, ui32> map =
						rank_doc_mass_map[last_rank - 1];
					ui32 total = 0;
					ui32 schrodinger_buff_c = 0;
					for (auto it = map.begin(); it != map.end(); ++it)
						if (masked_docs.find(it->first) == masked_docs.end()) {
							total += it->second;
							schrodinger_buff[schrodinger_buff_c++] =
								{it->second, it->first};
						}
					if (total == 0) {
						total = 1; ui32 doc = 0;
						fwrite(&total, sizeof(ui32), 1, stdout);
						fwrite(&doc, sizeof(ui32), 1, stdout);
						fwrite(&total, sizeof(ui32), 1, stdout);
					} else {
						fwrite(&total, sizeof(ui32), 1, stdout);
						qsort(
							schrodinger_buff,
							schrodinger_buff_c,
							sizeof(UI32Tuple),
							compare_ui32_tuple_desc
						);
						for (ui32 k = 0; k < schrodinger_buff_c; ++k) {
							if (
								masked_docs.find(schrodinger_buff[k].second)
								== masked_docs.end()
							) {
								ui32 prop = schrodinger_buff[k].first;
								ui32 doc = schrodinger_buff[k].second;
								fwrite(&doc, sizeof(ui32), 1, stdout);
								fwrite(&prop, sizeof(ui32), 1, stdout);
							}
						}
					}
				}
				ui32 total = 1;
				fwrite(&total, sizeof(ui32), 1, stdout);
				fwrite(&item.second, sizeof(ui32), 1, stdout);
				fwrite(&total, sizeof(ui32), 1, stdout);
			}
		}
	}
}
