#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <unordered_map>
#include <unordered_set>
#include <math.h>
#include "../stats.c"

typedef struct {
	ui32 total_mass;
	UI32Tuple *options;
	ui32 options_c;
} RankedOption;

typedef struct ViewBetweenClicksStruct {
	ui16 before;
	ui16 view;
	ui16 after;

    bool operator==(const ViewBetweenClicksStruct& other) const {
    	return
    		before == other.before
    		&& view == other.view
    		&& after == other.after;
    }
} ViewBetweenClicks;

typedef struct {
	ui16 before;
	ui16 view;
	ui16 after;
	probability p;
} ViewBetweenClicksResult;

struct ViewBetweenClicksHash {
    std::size_t operator()(const ViewBetweenClicks& k) const {
        return (((ui64) k.before) << 32) | (((ui64) k.view) << 16) | k.after;
    }
};

typedef struct QueryDocPairStruct {
	ui32 qid;
	ui32 doc;

	bool operator==(const QueryDocPairStruct& other) const {
		return qid == other.qid && doc == other.doc;
	}
} QueryDocPair;

typedef struct QueryDocPairHash {
	std::size_t operator()(const QueryDocPair& k) const {
		return (((ui64) k.qid) << 32) | k.doc;
	}
} QueryDocPairHash;

typedef struct {
	probability relevance;
	ui32 clicks;
} QueryDocMetric;

typedef struct {
	ui32 qid;
	ui32 doc;
	probability relevance;
	ui32 clicks;
} QueryDocResult;

typedef struct {
	ui32 doc;
	probability relevance;
} DocumentRelevance;

typedef struct {
	f64 numerator;
	f64 denominator;
} Fraction;

int compare_query_doc_result(const void *a, const void *b) {
	QueryDocResult *a_r = (QueryDocResult*) a;
	QueryDocResult *b_r = (QueryDocResult*) b;
	if (a_r->qid < b_r->qid) return -1;
	if (a_r->qid > b_r->qid) return +1;
	if (a_r->relevance < b_r->relevance) return +1;
	if (a_r->relevance > b_r->relevance) return -1;
	if (a_r->doc < b_r->doc) return -1;
	if (a_r->doc > b_r->doc) return +1;
	return 0;
}

int compare_view_prob(const void *a, const void *b) {
	ViewBetweenClicksResult *a_r = (ViewBetweenClicksResult*) a;
	ViewBetweenClicksResult *b_r = (ViewBetweenClicksResult*) b;
	if (a_r->after < b_r->after) return -1;
	if (a_r->after > b_r->after) return +1;
	if (a_r->before < b_r->before) return -1;
	if (a_r->before > b_r->before) return +1;
	if (a_r->view < b_r->view) return -1;
	if (a_r->view > b_r->view) return +1;
	if (a_r->p < b_r->p) return +1;
	if (a_r->p > b_r->p) return -1;
	return 0;
}

/**
 * Implementation of the click model described in:
 * Wang, C., Liu, Y., Wang, M., Zhou, K., Nie, J., & Ma, S. (2015).
 * Incorporating Non-sequential Behavior into Click Models.
 */
void partially_sequential_click_model() {
	ui8 file_opts;
	safe_read(&file_opts, sizeof(ui8), stdin);
	const bool is_probabilistic = file_opts == PROBABILISTIC_RANK_DATA;
	ui32 qid;
	ui32 session_c;
	ui32 duplicates;
	ui16 click_c;
	ui16 rank;
	ui32 doc;
	ui32 buff_size = 2000000000;
	char *buffer = (char*) malloc(buff_size);
	FILE *memstream = fmemopen(buffer, buff_size, "rb+");
	std::unordered_map<QueryDocPair, QueryDocMetric, QueryDocPairHash>
		rel_map;
	std::unordered_map<
		ViewBetweenClicks, probability, ViewBetweenClicksHash
			> view_prob_map;
	std::unordered_map<QueryDocPair, Fraction, QueryDocPairHash>
		relevance_fractions;
	std::unordered_map<ViewBetweenClicks, Fraction,
		ViewBetweenClicksHash> view_prob_fractions;
	ui64 qid_c = 0;
	bool *rank_clicked = (bool*) malloc(sizeof(bool) * MAX_RANK);
	for (ui32 i = 0; i < MAX_RANK; ++i) rank_clicked[i] = false;
	while (fread(&qid, sizeof(ui32), 1, stdin) == 1) {
		//fprintf(stderr, "QID: %u\n", qid);
		fwrite(&qid, sizeof(ui32), 1, memstream);
		safe_read(&session_c, sizeof(ui32), stdin);
		fwrite(&session_c, sizeof(ui32), 1, memstream);
		//fprintf(stderr, "SESSION_C: %u\n", session_c);
		for (ui32 i = 0; i < session_c; ++i) {
			safe_read(&duplicates, sizeof(ui32), stdin);
			//fprintf(stderr, "\tDUPLICATES: %u\n", duplicates);
			fwrite(&duplicates, sizeof(ui32), 1, memstream);
			safe_read(&click_c, sizeof(ui16), stdin);
			//fprintf(stderr, "\tCLICK_C: %u\n", click_c);
			fwrite(&click_c, sizeof(ui16), 1, memstream);
			ui16 prev_rank = 0;
			ui16 max_rank = 0;
			for (ui32 j = 0; j < click_c; ++j) {
				if (j == 0) {
					safe_read(&rank, sizeof(ui16), stdin);
					//fprintf(stderr, "\t\tFIRST RANK: %u\n", rank);
				} else {
					i8 gap;
					safe_read(&gap, sizeof(i8), stdin);
					//fprintf(stderr, "\t\tGAP: %d\n", gap);
					rank = prev_rank + gap;
				}
				rank_clicked[rank - 1] = true;
				fwrite(&rank, sizeof(ui16), 1, memstream);
				if (rank > max_rank) max_rank = rank;
				ui16 incr = prev_rank < rank ? 1 : -1;
				for (
					ui16 k = prev_rank == 0 ? 1 : prev_rank;
					k != rank;
					k += incr
				) {
					view_prob_map[ViewBetweenClicks{prev_rank, k, rank}] = 0.5;
					view_prob_fractions[ViewBetweenClicks{prev_rank, k, rank}]
						= {0, 0};
				}
				prev_rank = rank;
			}
			for (ui16 rank = 1; rank <= max_rank; ++rank) {
				if (is_probabilistic) {
					ui32 total;
					safe_read(&total, sizeof(ui32), stdin);
					fwrite(&total, sizeof(ui32), 1, memstream);
					ui32 prop;
					if (rank_clicked[rank - 1]) {
						rank_clicked[rank - 1] = false;
						// When a rank is clicked, its doc
						// must be known (no loop).
						ui32 doc;
						safe_read(&doc, sizeof(ui32), stdin);
						if (rel_map.find({qid, doc}) == rel_map.end()) {
							rel_map[{qid, doc}].relevance = 0.5;
							rel_map[{qid, doc}].clicks = 1;
						} else rel_map[{qid, doc}].clicks += 1;
						relevance_fractions[{qid, doc}] = {0, 0};
						fwrite(&doc, sizeof(ui32), 1, memstream);
						safe_read(&prop, sizeof(ui32), stdin);
						fwrite(&prop, sizeof(ui32), 1, memstream);
					} else {
						for (ui64 k = 0; k < total; k += prop) {
							ui32 doc;
							safe_read(&doc, sizeof(ui32), stdin);
							if (rel_map.find({qid, doc}) == rel_map.end()) {
								rel_map[{qid, doc}].relevance = 0.5;
								rel_map[{qid, doc}].clicks = 0;
							}
							relevance_fractions[{qid, doc}] = {0, 0};
							fwrite(&doc, sizeof(ui32), 1, memstream);
							safe_read(&prop, sizeof(ui32), stdin);
							fwrite(&prop, sizeof(ui32), 1, memstream);
						}
					}
				} else {
					ui32 doc;
					safe_read(&doc, sizeof(ui32), stdin);
					if (rank_clicked[rank - 1]) {
						rank_clicked[rank - 1] = false;
						if (rel_map.find({qid, doc}) == rel_map.end()) {
							rel_map[{qid, doc}].relevance = 0.5;
							rel_map[{qid, doc}].clicks = 1;
						} else rel_map[{qid, doc}].clicks += 1;
					} else {
						if (rel_map.find({qid, doc}) == rel_map.end()) {
							rel_map[{qid, doc}].relevance = 0.5;
							rel_map[{qid, doc}].clicks = 0;
						}
					}
					relevance_fractions[{qid, doc}] = {0, 0};
					fwrite(&doc, sizeof(ui32), 1, memstream);
				}
			}
		}
		qid_c++;
	}
	UI32Tuple *click_buff = (UI32Tuple*) malloc(sizeof(UI32Tuple) * 500);
	i16 click_buff_c;
	RankedOption* opt_buff =
		(RankedOption*) malloc(sizeof(RankedOption) * MAX_RANK);
	for (ui32 i = 0; i < MAX_RANK; ++i) {
		opt_buff[i].options_c = 0;
		opt_buff[i].options = (UI32Tuple*) malloc(sizeof(UI32Tuple) * 100000);
	}
	for (ui64 round = 0; round < 2; ++round) {
		fprintf(stderr, "EM ROUND: %lu\n", round);
		rewind(memstream);
		ui32 opt_buff_c = 0;
		for (ui64 qid_i = 0; qid_i < qid_c; ++qid_i) {
			safe_read(&qid, sizeof(ui32), memstream);
			safe_read(&session_c, sizeof(ui32), memstream);
			for (ui32 i = 0; i < session_c; ++i) {
				safe_read(&duplicates, sizeof(ui32), memstream);
				safe_read(&click_c, sizeof(ui16), memstream);
				click_buff_c = 0;
				ui16 max_rank = 0;
				for (; click_buff_c < click_c; ++click_buff_c) {
					safe_read(&rank, sizeof(ui16), memstream);
					click_buff[click_buff_c].first = rank;
					if (rank > max_rank) max_rank = rank;
				}
				opt_buff_c = 0;
				for (ui16 rank = 1; rank <= max_rank; ++rank) {
					ui32 total_mass;
					if (is_probabilistic)
						safe_read(&total_mass, sizeof(ui32), memstream);
					else total_mass = 1;
					opt_buff[opt_buff_c].total_mass = total_mass;
					opt_buff[opt_buff_c].options_c = 0;
					if (is_probabilistic) {
						ui32 prop;
						for (ui64 k = 0; k < total_mass; k += prop) {
							ui32 doc;
							safe_read(&doc, sizeof(ui32), memstream);
							safe_read(&prop, sizeof(ui32), memstream);
							ui32 options_c = opt_buff[opt_buff_c].options_c;
							opt_buff[opt_buff_c].options[options_c].first = doc;
							opt_buff[opt_buff_c].options[options_c].second = prop;
							opt_buff[opt_buff_c].options_c++;
						}
					} else {
						ui32 doc;
						safe_read(&doc, sizeof(ui32), memstream);
						ui32 options_c = opt_buff[opt_buff_c].options_c;
						opt_buff[opt_buff_c].options[options_c].first = doc;
						opt_buff[opt_buff_c].options_c++;
					}
					++opt_buff_c;
				}
				while (--click_buff_c >= 0)
					click_buff[click_buff_c].second = opt_buff[
						click_buff[click_buff_c].first - 1].options[0].first;
				ui16 prev_rank = 0;
				for (ui32 j = 0; j < click_c; ++j) {
					rank = click_buff[j].first;
					ui32 doc;
					ui16 incr = prev_rank < rank ? 1 : -1;
					for (
						ui16 k = prev_rank == 0 ? 1 : prev_rank;
						k != rank;
						k += incr
					) {
						ViewBetweenClicks key = {prev_rank, k, rank};
						const probability seen = view_prob_map[key];
						const probability not_seen = 1 - seen;
						if (is_probabilistic) {
							ui32 total_mass = opt_buff[k - 1].total_mass;
							for (ui16 l = 0; l < opt_buff[k - 1].options_c; ++l) {
								doc = opt_buff[k - 1].options[l].first;
								ui32 mass = opt_buff[k - 1].options[l].second;
								probability p = (double) mass / total_mass;
								probability rel = rel_map[{qid, doc}].relevance;
								probability no_click = 1 - (seen * rel);
								probability missed = not_seen * rel;
								// A1
								probability no_click_since_not_rel =
									(1 - rel) / no_click;
								relevance_fractions[{qid, doc}].denominator +=
									no_click_since_not_rel * duplicates * p;
								// A2
								probability no_click_since_missed =
									missed / no_click;
								Fraction *fraction =
									&relevance_fractions[{qid, doc}];
								fraction->numerator +=
									no_click_since_missed * duplicates * p;
								fraction->denominator +=
									no_click_since_missed * duplicates * p;
								// G1
								probability no_click_since_not_seen =
									not_seen / no_click;
								// G2
								probability no_click_since_seen_and_irrelevant =
									(seen * (1 - rel)) / no_click;
								fraction = &view_prob_fractions[key];
								fraction->numerator +=
									no_click_since_seen_and_irrelevant
									* duplicates * p;
								fraction->denominator +=
									no_click_since_seen_and_irrelevant
									* duplicates * p;
								fraction->denominator +=
									no_click_since_not_seen * duplicates * p;
							}
						} else {
							doc = opt_buff[k - 1].options[0].first;
							probability rel = rel_map[{qid, doc}].relevance;
							probability no_click = 1 - (seen * rel);
							probability missed = not_seen * rel;
							// A1
							probability no_click_since_not_rel =
								(1 - rel) / no_click;
							relevance_fractions[{qid, doc}].denominator +=
								no_click_since_not_rel * duplicates;
							// A2
							probability no_click_since_missed =
								missed / no_click;
							Fraction *fraction =
								&relevance_fractions[{qid, doc}];
							fraction->numerator +=
								no_click_since_missed * duplicates;
							fraction->denominator +=
								no_click_since_missed * duplicates;
							// G1
							probability no_click_since_not_seen =
								not_seen / no_click;
							// G2
							probability no_click_since_seen_and_irrelevant =
								(seen * (1 - rel)) / no_click;
							fraction = &view_prob_fractions[key];
							fraction->numerator +=
								no_click_since_seen_and_irrelevant
								* duplicates;
							fraction->denominator +=
								no_click_since_seen_and_irrelevant
								* duplicates;
							fraction->denominator +=
								no_click_since_not_seen * duplicates;
						}
					}
					doc = click_buff[j].second;
					// A3
					relevance_fractions[{qid, doc}].numerator += duplicates;
					relevance_fractions[{qid, doc}].denominator += duplicates;
					ViewBetweenClicks key = {prev_rank, rank, rank};
					// G3
					view_prob_fractions[key].numerator += duplicates;
					view_prob_fractions[key].denominator += duplicates;
					prev_rank = rank;
				}
			}
		}
		probability squared_error = 0;
		for (
			auto it = relevance_fractions.begin();
			it != relevance_fractions.end();
			++it
		) {
			QueryDocPair key = it->first;
			Fraction fraction = it->second;
			probability numerator = fraction.numerator;
			probability denominator = fraction.denominator;
			probability new_rel = numerator / (denominator + 0.01);
			probability old_rel = rel_map[key].relevance;
			squared_error += (new_rel - old_rel) * (new_rel - old_rel);
			rel_map[key].relevance = new_rel;
			it->second = {0, 0};
		}
		probability root_mean_squared_error =
			sqrt(squared_error / relevance_fractions.size());
		fprintf(stderr, "RMSE: %f\n", root_mean_squared_error);
		for (
			auto it = view_prob_fractions.begin();
			it != view_prob_fractions.end();
			++it
		) {
			ViewBetweenClicks key = it->first;
			Fraction fraction = it->second;
			probability numerator = fraction.numerator;
			probability denominator = fraction.denominator;
			view_prob_map[key] = numerator / (denominator + 0.01);
			it->second = {0, 0};
		}
	}
	QueryDocResult *result_buff =
		(QueryDocResult*) malloc(rel_map.size() * sizeof(QueryDocResult));
	ui64 result_buff_c = 0;
	for (auto it = rel_map.begin(); it != rel_map.end(); ++it)
		result_buff[result_buff_c++] = {
			it->first.qid,
			it->first.doc,
			it->second.relevance,
			it->second.clicks
		};
	qsort(
		result_buff,
		result_buff_c,
		sizeof(QueryDocResult),
		compare_query_doc_result
	);
	const f64 z = z_score(config().CM_confidence);
	const f64 z_inv_squared = (1 / z) * (1 / z);
	const f64 omega = z_inv_squared / (1 - z_inv_squared);
	fprintf(stderr, "OMEGA: %f\n", omega);
	ui32 prev_qid = 0;
	ui64 max_run = 0;
	ui64 current_run = 0;
	for (ui64 i = 0; i < result_buff_c; ++i) {
		if (prev_qid != result_buff[i].qid) {
			prev_qid = result_buff[i].qid;
			if (current_run > max_run) max_run = current_run;
			current_run = 0;
		}
		++current_run;
	}
	if (current_run > max_run) max_run = current_run;
	DocumentRelevance *doc_rel_buffer =
		(DocumentRelevance*) malloc(max_run * sizeof(DocumentRelevance));
	ui64 doc_rel_c = 0;
	for (ui64 i = 0; i < result_buff_c; ++i) {
		if (result_buff[i].doc == 0) continue;
		ui32 qid = result_buff[i].qid;
		if (qid != prev_qid) {
			if (doc_rel_c > 0) {
				fwrite(&prev_qid, sizeof(ui32), 1, stdout);
				fwrite(&doc_rel_c, sizeof(ui64), 1, stdout);
				for (ui64 j = 0; j < doc_rel_c; ++j) {
					ui32 doc = doc_rel_buffer[j].doc;
					probability relevance = doc_rel_buffer[j].relevance;
					fwrite(&doc, sizeof(ui32), 1, stdout);
					fwrite(&relevance, sizeof(probability), 1, stdout);
				}
				doc_rel_c = 0;
			}
			prev_qid = qid;
		}
		doc_rel_buffer[doc_rel_c++] = {
			result_buff[i].doc,
			result_buff[i].relevance
		};
		probability relevance = result_buff[i].relevance;
		ui32 clicks = result_buff[i].clicks;
		expected view_count = result_buff[i].clicks / result_buff[i].relevance;
		expected standard_error = sqrt(
				(
					(
						(clicks + omega) / (view_count + omega)
					)
					*
					(
						1 - clicks / (view_count + omega)
					)
				)
				/
				view_count
		);
		expected margin_of_error = z * standard_error;
		probability minimal_relevance = MAX(relevance - margin_of_error, 0);
		fprintf(
			stderr,
			"qid%u doc%u REL %f CLICKS: %u APPROX-VIEWS: %f MIN-REL: %f\n",
			qid,
			result_buff[i].doc,
			relevance,
			clicks,
			view_count,
			minimal_relevance
		);
	}
	fwrite(&prev_qid, sizeof(ui32), 1, stdout);
	fwrite(&doc_rel_c, sizeof(ui64), 1, stdout);
	for (ui64 j = 0; j < doc_rel_c; ++j) {
		ui32 doc = doc_rel_buffer[j].doc;
		probability relevance = doc_rel_buffer[j].relevance;
		fwrite(&doc, sizeof(ui32), 1, stdout);
		fwrite(&relevance, sizeof(probability), 1, stdout);
	}
}
