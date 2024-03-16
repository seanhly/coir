#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <unordered_map>
#include <unordered_set>
#include <math.h>
#include <stdint.h>
#include "../stats.c"
#include <vector>
#include <pthread.h>

template<typename T> using A = std::vector<T>;

const ui32 CLICK_FLAG = (ui32) 0xFFFFFFFF;

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
	ui32 d;

	bool operator==(const QueryDocPairStruct& other) const {
		return qid == other.qid && d == other.d;
	}
} QueryDocPair;

typedef struct QueryDocPairHash {
	std::size_t operator()(const QueryDocPair& k) const {
		return (((ui64) k.qid) << 32) | k.d;
	}
} QueryDocPairHash;

typedef struct {
	probability relevance;
	ui32 clicks;
	f64 *rel_numerator;
	f64 *rel_denominator;
} QueryDocMetrics;

typedef struct {
	ui32 qid;
	ui32 d;
	probability relevance;
	ui32 clicks;
} QueryDocResult;

typedef struct {
	ui32 d;
	probability relevance;
} DocumentRelevance;

typedef struct {
	f64 numerator;
	f64 denominator;
} Fraction;

typedef struct {
	f32 p;
	ui32 d;
	QueryDocMetrics *doc_metrics;
} TrainingRankOption;

typedef struct {
	TrainingRankOption* options;
	TrainingRankOption* after_options;
} TrainingFuzzyRank;

typedef struct {
	ui32 click_flag;
	ui32 d;
	QueryDocMetrics *doc_metrics;
} TrainingKnownRank;

typedef union {
	TrainingFuzzyRank rank;
	TrainingKnownRank click;
} TrainingPoint;

typedef struct {
	probability view_p;
	f64 *view_numerator;
	f64 *view_denominator;
} ViewMetrics;

typedef struct {
	ui16 rank;
	ViewMetrics *view_metrics;
} TrainingClick;

typedef struct {
	ui32 dupes;
	TrainingClick* clicks;
	TrainingClick* after_clicks;
	TrainingPoint* points;
	TrainingPoint* after_points;
} TrainingSession;

typedef struct {
	ui32 qid;
	TrainingSession *sessions;
	TrainingSession *after_sessions;
} Training;

typedef struct {
	A<Training> training_data;
	ui64 thread_i;
	bool is_probabilistic;
} TrainingMaterial;

int compare_query_doc_result(const void *a, const void *b) {
	QueryDocResult *a_r = (QueryDocResult*) a;
	QueryDocResult *b_r = (QueryDocResult*) b;
	if (a_r->qid < b_r->qid) return -1;
	if (a_r->qid > b_r->qid) return +1;
	if (a_r->relevance < b_r->relevance) return +1;
	if (a_r->relevance > b_r->relevance) return -1;
	if (a_r->d < b_r->d) return -1;
	if (a_r->d > b_r->d) return +1;
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

void *process_training_data_chunk(void *training_material) {
	ui16 rank;
	TrainingMaterial *m = (TrainingMaterial*) training_material;
	A<Training> training_data = m->training_data;
	bool is_probabilistic = m->is_probabilistic;
	ui64 chunk_size = training_data.size() / THREADS;
	ui64 start = m->thread_i * chunk_size;
	ui64 end;
	if (m->thread_i == THREADS - 1) end = training_data.size();
	else end = start + chunk_size;
	for (ui64 i = start; i < end; ++i) {
		Training dat = training_data[i];
		ui32 qid = dat.qid;
		for (TrainingSession *s = dat.sessions; s < dat.after_sessions; ++s) {
			ui32 dupes = s->dupes;
			ui16 max_rank = 0;
			ui16 prev_rank = 0;
			for (TrainingClick *click_ptr = s->clicks; click_ptr < s->after_clicks; ++click_ptr) {
				rank = click_ptr->rank;
				ViewMetrics *view_metrics = click_ptr->view_metrics;
				ui32 d;
				ui16 incr = prev_rank < rank ? 1 : -1;
				ui16 k = prev_rank == 0 ? 1 : prev_rank;
				for (; k != rank; k += incr, ++view_metrics) {
					TrainingPoint point = s->points[k - 1];
					const probability seen = view_metrics->view_p;
					const probability not_seen = 1 - seen;
					if (is_probabilistic && point.click.click_flag != CLICK_FLAG) {
						for (TrainingRankOption *o = point.rank.options; o < point.rank.after_options; ++o) {
							QueryDocMetrics *metrics = o->doc_metrics;
							probability rel = metrics->relevance;
							probability unclicked = 1 - (seen * rel);
							probability missed = not_seen * rel;
							// A1
							probability unclicked_since_not_rel =
								(1 - rel) / unclicked;
							metrics->rel_denominator[m->thread_i] +=
								unclicked_since_not_rel * dupes * o->p;
							// A2
							probability unclicked_since_missed =
								missed / unclicked;
							metrics->rel_numerator[m->thread_i] +=
								unclicked_since_missed * dupes * o->p;
							metrics->rel_denominator[m->thread_i] +=
								unclicked_since_missed * dupes * o->p;
							// G1
							probability unclicked_since_not_seen =
								not_seen / unclicked;
							// G2
							probability unclicked_since_seen_and_irrelevant
								= (seen * (1 - rel)) / unclicked;
							view_metrics->view_numerator[m->thread_i] +=
								unclicked_since_seen_and_irrelevant
								* dupes * o->p;
							view_metrics->view_denominator[m->thread_i] +=
								unclicked_since_seen_and_irrelevant
								* dupes * o->p;
							view_metrics->view_denominator[m->thread_i] +=
								unclicked_since_not_seen * dupes * o->p;
						}
					} else {
						d = point.click.d;
						QueryDocMetrics *metrics = point.click.doc_metrics;
						probability rel = metrics->relevance;
						probability unclicked = 1 - (seen * rel);
						probability missed = not_seen * rel;
						// A1
						probability unclicked_since_not_rel =
							(1 - rel) / unclicked;
						metrics->rel_denominator[m->thread_i] +=
							unclicked_since_not_rel * dupes;
						// A2
						probability unclicked_since_missed =
							missed / unclicked;
						metrics->rel_numerator[m->thread_i] +=
							unclicked_since_missed * dupes;
						metrics->rel_denominator[m->thread_i] +=
							unclicked_since_missed * dupes;
						// G1
						probability unclicked_since_not_seen =
							not_seen / unclicked;
						// G2
						probability unclicked_since_seen_and_irrelevant =
							(seen * (1 - rel)) / unclicked;
						view_metrics->view_numerator[m->thread_i] +=
							unclicked_since_seen_and_irrelevant * dupes;
						view_metrics->view_denominator[m->thread_i] +=
							unclicked_since_seen_and_irrelevant * dupes;
						view_metrics->view_denominator[m->thread_i] +=
							unclicked_since_not_seen * dupes;
					}
				}
				d = s->points[rank - 1].click.d;
				QueryDocMetrics *metrics =
					s->points[rank - 1].click.doc_metrics;
				// A3
				metrics->rel_numerator[m->thread_i] += dupes;
				metrics->rel_denominator[m->thread_i] += dupes;
				// G3
				view_metrics->view_numerator[m->thread_i] += dupes;
				view_metrics->view_denominator[m->thread_i] += dupes;
				prev_rank = rank;
			}
		}
	}
	return NULL;
}

/**
 * Implementation of the click model described in:
 * Wang, C., Liu, Y., Wang, M., Zhou, K., Nie, J., & Ma, S. (2015).
 * Incorporating Non-sequential Behavior into Click Models.
 */
void partially_sequential_click_model() {
	ui8 file_opts;
	safe_read(&file_opts, sizeof(ui8), stdin);
	const f64 z = z_score(config().CM_confidence);
	const bool is_probabilistic = file_opts == PROBABILISTIC_RANK_DATA;
	ui32 qid;
	ui32 session_c;
	ui32 dupes;
	ui16 click_c;
	ui16 rank;
	ui32 d;
	ui32 buff_size = 2000000000;
	char *buffer = (char*) malloc(buff_size);
	FILE *memstream = fmemopen(buffer, buff_size, "rb+");
	std::unordered_map<QueryDocPair, QueryDocMetrics, QueryDocPairHash>
		rel_map;
	std::unordered_map<ViewBetweenClicks, ViewMetrics*, ViewBetweenClicksHash>
		view_map;
	ui64 qid_c = 0;
	bool *rank_clicked = (bool*) malloc(sizeof(bool) * MAX_RANK);
	for (ui32 i = 0; i < MAX_RANK; ++i) rank_clicked[i] = false;
	A<Training> training_data;
	A<TrainingSession> training_sessions;
	A<TrainingClick> training_data_clicks;
	A<TrainingPoint> training_points;
	A<TrainingRankOption> training_data_rank_options;
	A<ViewMetrics> training_view_metrics;
	training_data.reserve(13279574);
	training_sessions.reserve(21633169);
	training_data_clicks.reserve(46296250);
	training_points.reserve(98308810);
	training_data_rank_options.reserve(98308810);
	training_view_metrics.reserve(98308810);
	f64 *numens_and_denoms = (f64*) malloc(sizeof(f64) * THREADS * 2 * 98308810);
	ui64 numens_and_denoms_c = 0;
	while (fread(&qid, sizeof(ui32), 1, stdin) == 1) {
		safe_read(&session_c, sizeof(ui32), stdin);
		Training data = {
			qid,
			training_sessions.data() + training_sessions.size(),
			NULL
		};
		for (ui32 i = 0; i < session_c; ++i) {
			safe_read(&dupes, sizeof(ui32), stdin);
			safe_read(&click_c, sizeof(ui16), stdin);
			TrainingSession session = {
				dupes,
				training_data_clicks.data() + training_data_clicks.size(),
				NULL,
				training_points.data() + training_points.size(),
				NULL
			};
			TrainingClick *prev_click = NULL;
			ui16 max_rank = 0;
			for (ui32 j = 0; j < click_c; ++j) {
				if (j == 0) {
					safe_read(&rank, sizeof(ui16), stdin);
				} else {
					i8 hop;
					safe_read(&hop, sizeof(i8), stdin);
					rank = prev_click->rank + hop;
				}
				rank_clicked[rank - 1] = true;
				if (rank > max_rank) max_rank = rank;
				i16 incr;
				ui16 k;
				ui16 prev_rank;
				if (prev_click == NULL) {
					k = 1;
					prev_rank = 0;
					incr = 1;
				} else {
					k = prev_click->rank;
					prev_rank = prev_click->rank;
					incr = prev_rank <= rank ? 1 : -1;
				}
				ViewMetrics *metrics;
				if (view_map.find({prev_rank, k, rank}) == view_map.end()) {
					metrics = training_view_metrics.data() + training_view_metrics.size();
					view_map[ViewBetweenClicks{prev_rank, k, rank}] = metrics;
					for (; k != rank + incr; k += incr) {
						ViewMetrics metric;
						metric.view_p = 0.5;
						metric.view_numerator = numens_and_denoms + numens_and_denoms_c;
						for (ui64 thread_i = 0; thread_i < THREADS; ++thread_i)
							numens_and_denoms[numens_and_denoms_c++] = 0;
						metric.view_denominator = numens_and_denoms + numens_and_denoms_c;
						for (ui64 thread_i = 0; thread_i < THREADS; ++thread_i)
							numens_and_denoms[numens_and_denoms_c++] = 0;
						training_view_metrics.push_back(metric);
					}
				} else metrics = view_map[{prev_rank, k, rank}];
				TrainingClick click = {rank, metrics};
				training_data_clicks.push_back(click);
				prev_click = &training_data_clicks[training_data_clicks.size() - 1];
			}
			session.after_clicks =
				training_data_clicks.data() + training_data_clicks.size();
			for (ui16 rank = 1; rank <= max_rank; ++rank) {
				if (is_probabilistic) {
					ui32 total;
					safe_read(&total, sizeof(ui32), stdin);
					ui32 prop;
					if (rank_clicked[rank - 1]) {
						rank_clicked[rank - 1] = false;
						ui32 d;
						safe_read(&d, sizeof(ui32), stdin);
						safe_read(&prop, sizeof(ui32), stdin);
						if (rel_map.find({qid, d}) == rel_map.end()) {
							rel_map[{qid, d}].relevance = 0.5;
							rel_map[{qid, d}].clicks = 1;
							rel_map[{qid, d}].rel_numerator = numens_and_denoms + numens_and_denoms_c;
							for (ui64 thread_i = 0; thread_i < THREADS; ++thread_i)
								numens_and_denoms[numens_and_denoms_c++] = 0;
							rel_map[{qid, d}].rel_denominator = numens_and_denoms + numens_and_denoms_c;
							for (ui64 thread_i = 0; thread_i < THREADS; ++thread_i)
								numens_and_denoms[numens_and_denoms_c++] = 0;
						}
						else rel_map[{qid, d}].clicks += 1;
						TrainingPoint point;
						QueryDocMetrics *metrics = &rel_map[{qid, d}];
						point.click = {CLICK_FLAG, d, metrics};
						training_points.push_back(point);
					} else {
						TrainingPoint point;
						point.rank.options =
							training_data_rank_options.data() +
								training_data_rank_options.size();
						for (ui64 k = 0; k < total; k += prop) {
							ui32 d;
							safe_read(&d, sizeof(ui32), stdin);
							if (rel_map.find({qid, d}) == rel_map.end()) {
								rel_map[{qid, d}].relevance = 0.5;
								rel_map[{qid, d}].clicks = 0;
								rel_map[{qid, d}].rel_numerator = numens_and_denoms + numens_and_denoms_c;
								for (ui64 thread_i = 0; thread_i < THREADS; ++thread_i)
									numens_and_denoms[numens_and_denoms_c++] = 0;
								rel_map[{qid, d}].rel_denominator = numens_and_denoms + numens_and_denoms_c;
								for (ui64 thread_i = 0; thread_i < THREADS; ++thread_i)
									numens_and_denoms[numens_and_denoms_c++] = 0;
							}
							safe_read(&prop, sizeof(ui32), stdin);
							QueryDocMetrics *metrics = &rel_map[{qid, d}];
							training_data_rank_options.push_back({
								(f32) prop / (f32) total, d, metrics
							});
						}
						point.rank.after_options =
							training_data_rank_options.data() +
								training_data_rank_options.size();
						training_points.push_back(point);
					}
				} else {
					ui32 d;
					safe_read(&d, sizeof(ui32), stdin);
					if (rank_clicked[rank - 1]) {
						rank_clicked[rank - 1] = false;
						if (rel_map.find({qid, d}) == rel_map.end()) {
							rel_map[{qid, d}].relevance = 0.5;
							rel_map[{qid, d}].clicks = 1;
							rel_map[{qid, d}].rel_numerator = numens_and_denoms + numens_and_denoms_c;
							for (ui64 thread_i = 0; thread_i < THREADS; ++thread_i)
								numens_and_denoms[numens_and_denoms_c++] = 0;
							rel_map[{qid, d}].rel_denominator = numens_and_denoms + numens_and_denoms_c;
							for (ui64 thread_i = 0; thread_i < THREADS; ++thread_i)
								numens_and_denoms[numens_and_denoms_c++] = 0;
						} else rel_map[{qid, d}].clicks += 1;
						QueryDocMetrics *metrics = &rel_map[{qid, d}];
						TrainingPoint point;
						point.click = {CLICK_FLAG, d, metrics};
						training_points.push_back(point);
					} else {
						if (rel_map.find({qid, d}) == rel_map.end()) {
							rel_map[{qid, d}].relevance = 0.5;
							rel_map[{qid, d}].clicks = 0;
							rel_map[{qid, d}].rel_numerator = numens_and_denoms + numens_and_denoms_c;
							for (ui64 thread_i = 0; thread_i < THREADS; ++thread_i)
								numens_and_denoms[numens_and_denoms_c++] = 0;
							rel_map[{qid, d}].rel_denominator = numens_and_denoms + numens_and_denoms_c;
							for (ui64 thread_i = 0; thread_i < THREADS; ++thread_i)
								numens_and_denoms[numens_and_denoms_c++] = 0;
						}
						TrainingPoint point;
						QueryDocMetrics *metrics = &rel_map[{qid, d}];
						point.click = {CLICK_FLAG, d, metrics};
						training_points.push_back(point);
					}
				}
			}
			session.after_points =
				training_points.data() + training_points.size();
			training_sessions.push_back(session);
		}
		data.after_sessions =
			training_sessions.data() + training_sessions.size();
		training_data.push_back(data);
		qid_c++;
	}
	pthread_t threads[THREADS];
	TrainingMaterial training_materials[THREADS];
	i32 rc;
	for (ui64 round = 0; round < 1000; ++round) {
		fprintf(stderr, "EM ROUND: %lu\n", round);
		for (ui64 t = 0; t < THREADS; ++t) {
			training_materials[t].training_data = training_data;
			training_materials[t].is_probabilistic = is_probabilistic;
			training_materials[t].thread_i = t;
			rc = pthread_create(
				&threads[t],
				NULL,
				process_training_data_chunk,
				(void*) &training_materials[t]
			);
		}
		for (ui64 t = 0; t < THREADS; ++t)
			pthread_join(threads[t], NULL);
		probability squared_error = 0;
		probability max_error = 0;
		for (auto it = rel_map.begin(); it != rel_map.end(); ++it) {
			probability numerator = 0;
			probability denominator = 0;
			for (ui64 i = 0; i < THREADS; ++i) {
				numerator += it->second.rel_numerator[i];
				it->second.rel_numerator[i] = 0;
				denominator += it->second.rel_denominator[i];
				it->second.rel_denominator[i] = 0;
			}
			probability new_rel = numerator / (denominator + 0.01);
			probability old_rel = it->second.relevance;
			probability error = new_rel - old_rel;
			if (fabs(error) > max_error) max_error = fabs(error);
			squared_error += error * error;
			it->second.relevance = new_rel;
		}
		probability root_mean_squared_error =
			sqrt(squared_error / rel_map.size());
		fprintf(
			stderr,
			"RMSE (Relevance): %0.10f, Max error: %0.10f\n",
			root_mean_squared_error, max_error);
		squared_error = 0;
		max_error = 0;
		for (
			auto it = training_view_metrics.begin();
			it != training_view_metrics.end();
			++it
		) {
			probability numerator = 0;
			probability denominator = 0;
			for (ui64 i = 0; i < THREADS; ++i) {
				numerator += it->view_numerator[i];
				it->view_numerator[i] = 0;
				denominator += it->view_denominator[i];
				it->view_denominator[i] = 0;
			}
			probability old_prob = it->view_p;
			probability new_prob = numerator / (denominator + 0.01);
			probability error = new_prob - old_prob;
			squared_error += error * error;
			if (fabs(error) > max_error) max_error = fabs(error);
			it->view_p = new_prob;
		}
		root_mean_squared_error =
			sqrt(squared_error / training_view_metrics.size());
		fprintf(
			stderr,
			"RMSE (Hops): %0.10f, Max error: %0.10f\n",
			root_mean_squared_error,
			max_error
		);
	}
	QueryDocResult *result_buff =
		(QueryDocResult*) malloc(rel_map.size() * sizeof(QueryDocResult));
	ui64 result_buff_c = 0;
	for (auto it = rel_map.begin(); it != rel_map.end(); ++it)
		result_buff[result_buff_c++] = {
			it->first.qid,
			it->first.d,
			it->second.relevance,
			it->second.clicks
		};
	qsort(
		result_buff,
		result_buff_c,
		sizeof(QueryDocResult),
		compare_query_doc_result
	);
	const f64 z_inv_squared = (1 / z) * (1 / z);
	const f64 omega = z_inv_squared / (1 - z_inv_squared);
	fprintf(stderr, "OMEGA: %0.10f\n", omega);
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
		if (result_buff[i].d == 0) continue;
		ui32 qid = result_buff[i].qid;
		if (qid != prev_qid) {
			if (doc_rel_c > 0) {
				fwrite(&prev_qid, sizeof(ui32), 1, stdout);
				fwrite(&doc_rel_c, sizeof(ui64), 1, stdout);
				for (ui64 j = 0; j < doc_rel_c; ++j) {
					ui32 d = doc_rel_buffer[j].d;
					probability relevance = doc_rel_buffer[j].relevance;
					fwrite(&d, sizeof(ui32), 1, stdout);
					fwrite(&relevance, sizeof(probability), 1, stdout);
				}
				doc_rel_c = 0;
			}
			prev_qid = qid;
		}
		doc_rel_buffer[doc_rel_c++] = {
			result_buff[i].d,
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
		if (minimal_relevance > 0.000001) {
			fprintf(
				stderr,
				"qid%u d%u REL %f CLICKS: %u APPROX-VIEWS: %f MIN-REL: %f\n",
				qid,
				result_buff[i].d,
				relevance,
				clicks,
				view_count,
				minimal_relevance
			);
		}
	}
	fwrite(&prev_qid, sizeof(ui32), 1, stdout);
	fwrite(&doc_rel_c, sizeof(ui64), 1, stdout);
	for (ui64 j = 0; j < doc_rel_c; ++j) {
		ui32 d = doc_rel_buffer[j].d;
		probability relevance = doc_rel_buffer[j].relevance;
		fwrite(&d, sizeof(ui32), 1, stdout);
		fwrite(&relevance, sizeof(probability), 1, stdout);
	}
}
