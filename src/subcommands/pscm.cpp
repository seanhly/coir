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

const ui32 CLICK_FLAG = (ui32) 0xFFFFFFFF;

template<typename T> using A = std::vector<T>;

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
	ui32 clicks;
	probability relevance;
} DocumentClicksRelevance;

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
	ui64 certain_view_c;
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
	Training *training_data;
	ui64 training_data_c;
	ui64 thread_i;
	bool is_probabilistic;
	pthread_barrier_t *checkpoint_one;
	f64 squared_error_rel;
	f64 squared_error_view;
	f64 max_error_rel;
	f64 max_error_view;
	QueryDocMetrics *query_doc_metrics;
	ui64 query_doc_metrics_c;
	ViewMetrics *view_metrics;
	ui64 view_metrics_c;
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

void *process_training_data_chunk(void *training_material) {
	// =========================================================//
	// -- STEP 1: Apply the model to the training data chunk -- //
	// =========================================================//
	ui16 rank;
	TrainingMaterial *m = (TrainingMaterial*) training_material;
	Training* training_data = m->training_data;
	bool is_probabilistic = m->is_probabilistic;
	ui64 chunk_size = m->training_data_c / THREADS;
	ui64 start = m->thread_i * chunk_size;
	ui64 end;
	if (m->thread_i == THREADS - 1) end = m->training_data_c;
	else end = start + chunk_size;
	for (ui64 i = start; i < end; ++i) {
		Training dat = training_data[i];
		ui32 qid = dat.qid;
		for (TrainingSession *s = dat.sessions; s < dat.after_sessions; ++s) {
			ui32 dupes = s->dupes;
			ui16 max_rank = 0;
			ui16 prev_rank = 0;
			for (TrainingClick *click_ptr = s->clicks;
					click_ptr < s->after_clicks; ++click_ptr) {
				rank = click_ptr->rank;
				ViewMetrics *view_metrics = click_ptr->view_metrics;
				ui32 d;
				ui16 incr = prev_rank < rank ? 1 : -1;
				ui16 k = prev_rank == 0 ? 1 : prev_rank;
				for (; k != rank; k += incr, ++view_metrics) {
					TrainingPoint point = s->points[k - 1];
					fflush(stdout);
					const probability seen = view_metrics->view_p;
					const probability not_seen = 1 - seen;
					if (is_probabilistic && point.click.click_flag
							!= CLICK_FLAG) {
						for (TrainingRankOption *o = point.rank.options;
								o < point.rank.after_options; ++o) {
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
				/*
				 * // G3
				 * view_metrics->view_numerator[m->thread_i] += dupes;
				 * view_metrics->view_denominator[m->thread_i] += dupes;
				 *
				 * QueryDocMetrics *metrics =
				 *     s->points[rank - 1].click.doc_metrics;
				 * // A3
				 * metrics->rel_numerator[m->thread_i] += dupes;
				 * metrics->rel_denominator[m->thread_i] += dupes;
				 * 
				 * ... the above five SLOCs were optimised away ...
				 * 
				 * ... see how 'click' counts are initially placed in the
				 * numerator and denominator when the threads are getting
				 * merged in the evaluation phase.
				 */
				prev_rank = rank;
			}
		}
	}
	pthread_barrier_wait(m->checkpoint_one);
	// ==============================================================//
	// -- STEP 2: Update relevance, calculate RMSE, re-initialise -- //
	// ==============================================================//
	QueryDocMetrics *global_query_doc_metrics = m->query_doc_metrics;
	chunk_size = m->query_doc_metrics_c / THREADS;
	start = m->thread_i * chunk_size;
	end = m->thread_i == THREADS - 1 ?
		m->query_doc_metrics_c : start + chunk_size;
	probability squared_error = 0;
	probability max_error = 0;
	for (ui64 i = start; i < end; ++i) {
		QueryDocMetrics *mm = &global_query_doc_metrics[i];
		probability numerator = mm->clicks;
		probability denominator = mm->clicks;
		for (ui64 i = 0; i < THREADS; ++i) {
			numerator += mm->rel_numerator[i];
			mm->rel_numerator[i] = 0;
			denominator += mm->rel_denominator[i];
			mm->rel_denominator[i] = 0;
		}
		probability new_rel = numerator / (denominator + 0.01);
		probability old_rel = mm->relevance;
		probability error = new_rel - old_rel;
		if (fabs(error) > max_error) max_error = fabs(error);
		squared_error += error * error;
		mm->relevance = new_rel;
	}
	m->squared_error_rel = squared_error;
	m->max_error_rel = max_error;
	ViewMetrics *global_view_metrics = m->view_metrics;
	chunk_size = m->view_metrics_c / THREADS;
	start = m->thread_i * chunk_size;
	end = m->thread_i == THREADS - 1 ? m->view_metrics_c : start + chunk_size;
	squared_error = 0;
	max_error = 0;
	for (ui64 i = start; i < end; ++i) {
		ViewMetrics *mm = &global_view_metrics[i];
		probability numerator = mm->certain_view_c;
		probability denominator = mm->certain_view_c;
		for (ui64 i = 0; i < THREADS; ++i) {
			numerator += mm->view_numerator[i];
			mm->view_numerator[i] = 0;
			denominator += mm->view_denominator[i];
			mm->view_denominator[i] = 0;
		}
		probability old_prob = mm->view_p;
		probability new_prob = numerator / (denominator + 0.01);
		probability error = new_prob - old_prob;
		squared_error += error * error;
		if (fabs(error) > max_error) max_error = fabs(error);
		mm->view_p = new_prob;
	}
	m->squared_error_view = squared_error;
	m->max_error_view = max_error;
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
	const bool is_probabilistic = file_opts == PROBABILISTIC_RANK_DATA;
	ui32 dupes;
	ui16 click_c;
	ui16 rank;
	ui32 d;
	ui32 buff_size = 2000000000;
	char *buffer = (char*) malloc(buff_size);
	FILE *memstream = fmemopen(buffer, buff_size, "rb+");
	std::unordered_map<QueryDocPair, QueryDocMetrics*, QueryDocPairHash>
		rel_map;
	std::unordered_map<ViewBetweenClicks, ViewMetrics*, ViewBetweenClicksHash>
		view_map;
	ui16 *rank_clicks = (ui16*) malloc(sizeof(ui16) * MAX_RANK);
	for (ui32 i = 0; i < MAX_RANK; ++i) rank_clicks[i] = 0;
	A<Training> training_data;
	A<TrainingSession> training_sessions;
	A<TrainingClick> training_data_clicks;
	A<TrainingPoint> training_points;
	A<TrainingRankOption> training_data_rank_options;
	A<ViewMetrics> view_metrics;
	A<QueryDocMetrics> query_doc_metrics;
	training_data.reserve(13279574);
	training_sessions.reserve(21633169);
	training_data_clicks.reserve(46296250);
	training_points.reserve(98308810);
	training_data_rank_options.reserve(98308810);
	query_doc_metrics.reserve(98308810);
	view_metrics.reserve(98308810);
	f64 *numens_and_denoms =
		(f64*) malloc(sizeof(f64) * THREADS * 2L * (13279574L + 45440874L));
	ui64 numens_and_denoms_c = 0;
	ui32 qid = 1;
	ui32 session_c;
	while (fread(&session_c, sizeof(ui32), 1, stdin) == 1) {
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
			ui16 prev_click = 0;
			ui16 max_rank = 0;
			for (ui32 j = 0; j < click_c; ++j) {
				if (j == 0) {
					safe_read(&rank, sizeof(ui16), stdin);
				} else {
					i8 hop;
					safe_read(&hop, sizeof(i8), stdin);
					rank = prev_click + hop;
				}
				if (rank_clicks[rank - 1] != 0xFFFF) ++rank_clicks[rank - 1];
				if (rank > max_rank) max_rank = rank;
				i16 incr;
				ui16 k;
				ui16 prev_rank;
				if (prev_click == 0) {
					k = 1;
					prev_rank = 0;
					incr = 1;
				} else {
					k = prev_click;
					prev_rank = prev_click;
					incr = prev_rank <= rank ? 1 : -1;
				}
				ViewMetrics *metrics;
				if (view_map.find({prev_rank, k, rank}) == view_map.end()) {
					metrics = view_metrics.data() + view_metrics.size();
					view_map[ViewBetweenClicks{prev_rank, k, rank}] = metrics;
					for (; k != rank + incr; k += incr) {
						ViewMetrics metric;
						metric.view_p = 0.5;
						metric.certain_view_c = k == rank ? dupes : 0;
						metric.view_numerator =
							numens_and_denoms + numens_and_denoms_c;
						for (ui64 t_i = 0; t_i < THREADS; ++t_i)
							numens_and_denoms[numens_and_denoms_c++] = 0;
						metric.view_denominator =
							numens_and_denoms + numens_and_denoms_c;
						for (ui64 thread_i = 0; thread_i < THREADS; ++thread_i)
							numens_and_denoms[numens_and_denoms_c++] = 0;
						view_metrics.push_back(metric);
					}
				} else {
					metrics = view_map[{prev_rank, k, rank}];
					ViewMetrics *metric_at_rank;
					metric_at_rank = metrics + abs(k - rank);
					metric_at_rank->certain_view_c += dupes;
				}
				training_data_clicks.push_back({rank, metrics});
				prev_click = rank;
			}
			session.after_clicks =
				training_data_clicks.data() + training_data_clicks.size();
			for (ui16 rank = 1; rank <= max_rank; ++rank) {
				if (is_probabilistic) {
					ui32 total;
					safe_read(&total, sizeof(ui32), stdin);
					ui32 prop;
					if (rank_clicks[rank - 1]) {
						ui32 d;
						safe_read(&d, sizeof(ui32), stdin);
						safe_read(&prop, sizeof(ui32), stdin);
						QueryDocMetrics *metrics_ptr;
						if (rel_map.find({qid, d}) == rel_map.end()) {
							QueryDocMetrics metrics;
							metrics.relevance = 0.5;
							metrics.clicks = dupes * rank_clicks[rank - 1];
							metrics.rel_numerator =
								numens_and_denoms + numens_and_denoms_c;
							for (ui64 thread_i = 0; thread_i < THREADS;
									++thread_i)
								numens_and_denoms[numens_and_denoms_c++] = 0;
							metrics.rel_denominator =
								numens_and_denoms + numens_and_denoms_c;
							for (ui64 thread_i = 0; thread_i < THREADS;
									++thread_i)
								numens_and_denoms[numens_and_denoms_c++] = 0;
							query_doc_metrics.push_back(metrics);
							metrics_ptr = &query_doc_metrics[
								query_doc_metrics.size() - 1
							];
							rel_map[{qid, d}] = metrics_ptr;
						} else {
							metrics_ptr = rel_map[{qid, d}];
							metrics_ptr->clicks +=
								dupes * rank_clicks[rank - 1];
						}
						TrainingPoint point;
						point.click = {CLICK_FLAG, d, metrics_ptr};
						training_points.push_back(point);
					} else {
						TrainingPoint point;
						point.rank.options =
							training_data_rank_options.data() +
								training_data_rank_options.size();
						for (ui64 k = 0; k < total; k += prop) {
							ui32 d;
							safe_read(&d, sizeof(ui32), stdin);
							safe_read(&prop, sizeof(ui32), stdin);
							QueryDocMetrics *metrics_ptr;
							f32 doc_p = (f32) prop / (f32) total;
							if (rel_map.find({qid, d}) == rel_map.end()) {
								QueryDocMetrics metrics;
								metrics.relevance = 0.5;
								metrics.clicks = 0;
								metrics.rel_numerator =
									numens_and_denoms + numens_and_denoms_c;
								for (ui64 thread_i = 0; thread_i < THREADS;
										++thread_i)
									numens_and_denoms[numens_and_denoms_c++] =
										0;
								metrics.rel_denominator =
									numens_and_denoms + numens_and_denoms_c;
								for (ui64 thread_i = 0; thread_i < THREADS;
										++thread_i)
									numens_and_denoms[numens_and_denoms_c++] =
										0;
								query_doc_metrics.push_back(metrics);
								metrics_ptr = &query_doc_metrics[
									query_doc_metrics.size() - 1
								];
								rel_map[{qid, d}] = metrics_ptr;
							} else metrics_ptr = rel_map[{qid, d}];
							training_data_rank_options.push_back({
								doc_p, d, metrics_ptr
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
					if (rank_clicks[rank - 1]) {
						QueryDocMetrics *metrics_ptr;
						if (rel_map.find({qid, d}) == rel_map.end()) {
							QueryDocMetrics metrics;
							metrics.relevance = 0.5;
							metrics.clicks = dupes * rank_clicks[rank - 1];
							metrics.rel_numerator =
								numens_and_denoms + numens_and_denoms_c;
							for (ui64 thread_i = 0; thread_i < THREADS;
									++thread_i)
								numens_and_denoms[numens_and_denoms_c++] = 0;
							metrics.rel_denominator =
								numens_and_denoms + numens_and_denoms_c;
							for (ui64 thread_i = 0; thread_i < THREADS;
									++thread_i)
								numens_and_denoms[numens_and_denoms_c++] = 0;
							query_doc_metrics.push_back(metrics);
							metrics_ptr = &query_doc_metrics[
								query_doc_metrics.size() - 1
							];
							rel_map[{qid, d}] = metrics_ptr;
						} else {
							metrics_ptr = rel_map[{qid, d}];
							metrics_ptr->clicks += dupes
								* rank_clicks[rank - 1];
						}
						TrainingPoint point;
						point.click = {CLICK_FLAG, d, metrics_ptr};
						training_points.push_back(point);
					} else {
						QueryDocMetrics *metrics_ptr;
						if (rel_map.find({qid, d}) == rel_map.end()) {
							QueryDocMetrics metrics;
							metrics.relevance = 0.5;
							metrics.clicks = 0;
							metrics.rel_numerator =
								numens_and_denoms + numens_and_denoms_c;
							for (ui64 thread_i = 0; thread_i < THREADS;
									++thread_i)
								numens_and_denoms[numens_and_denoms_c++] = 0;
							metrics.rel_denominator =
								numens_and_denoms + numens_and_denoms_c;
							for (ui64 thread_i = 0; thread_i < THREADS;
									++thread_i)
								numens_and_denoms[numens_and_denoms_c++] = 0;
							query_doc_metrics.push_back(metrics);
							metrics_ptr = &query_doc_metrics[
								query_doc_metrics.size() - 1
							];
							rel_map[{qid, d}] = metrics_ptr;
						} else metrics_ptr = rel_map[{qid, d}];
						TrainingPoint point;
						point.click = {CLICK_FLAG, d, metrics_ptr};
						training_points.push_back(point);
					}
				}
				rank_clicks[rank - 1] = 0;
			}
			session.after_points =
				training_points.data() + training_points.size();
			training_sessions.push_back(session);
		}
		data.after_sessions =
			training_sessions.data() + training_sessions.size();
		training_data.push_back(data);
		++qid;
	}
	pthread_t threads[THREADS];
	TrainingMaterial training_materials[THREADS];
	pthread_barrier_t checkpoint_one;
	pthread_barrier_init(&checkpoint_one, NULL, THREADS);
	i32 rc;
	for (ui64 t = 0; t < THREADS; ++t) {
		training_materials[t].training_data = training_data.data();
		training_materials[t].training_data_c = training_data.size();
		training_materials[t].is_probabilistic = is_probabilistic;
		training_materials[t].thread_i = t;
		training_materials[t].checkpoint_one = &checkpoint_one;
		training_materials[t].query_doc_metrics = query_doc_metrics.data();
		training_materials[t].query_doc_metrics_c = query_doc_metrics.size();
		training_materials[t].view_metrics = view_metrics.data();
		training_materials[t].view_metrics_c = view_metrics.size();
		training_materials[t].squared_error_rel = 0;
		training_materials[t].squared_error_view = 0;
	}
	for (ui64 round = 0; round < 10; ++round) {
		fprintf(stderr, "[%lu]", round);
		for (ui64 r = round | 1; r <= 9999; r *= 10) fputs(" ", stderr);
		fprintf(stderr, "...");
		for (ui64 t = 1; t < THREADS; ++t)
			rc = pthread_create(
				&threads[t],
				NULL,
				process_training_data_chunk,
				(void*) &training_materials[t]
			);
		process_training_data_chunk((void*) &training_materials[0]);
		f64 squared_error_rel = training_materials[0].squared_error_rel;
		f64 squared_error_view = training_materials[0].squared_error_view;
		f64 max_error_rel = training_materials[0].max_error_rel;
		f64 max_error_view = training_materials[0].max_error_view;
		for (ui64 t = 1; t < THREADS; ++t) {
			pthread_join(threads[t], NULL);
			squared_error_rel += training_materials[t].squared_error_rel;
			squared_error_view += training_materials[t].squared_error_view;
			if (training_materials[t].max_error_rel > max_error_rel)
				max_error_rel = training_materials[t].max_error_rel;
			if (training_materials[t].max_error_view > max_error_view)
				max_error_view = training_materials[t].max_error_view;
		}
		fprintf(stderr, "\b\b\b");
		fprintf(
			stderr,
			"%sRMSE %s⍺%s:%0.8f %sγ%s:%0.8f%s | %sMax(E) %s⍺%s:%0.8f %sγ%s:%0.8f\n",
			GREEN,
			YELLOW,
			RESET,
			sqrt(squared_error_rel / query_doc_metrics.size()),
			YELLOW,
			RESET,
			sqrt(squared_error_view / view_metrics.size()),
			RESET,
			GREEN,
			YELLOW,
			RESET,
			max_error_rel,
			YELLOW,
			RESET,
			max_error_view
		);
	}
	QueryDocResult *result_buff =
		(QueryDocResult*) malloc(rel_map.size() * sizeof(QueryDocResult));
	ui64 result_buff_c = 0;
	for (auto it = rel_map.begin(); it != rel_map.end(); ++it)
		result_buff[result_buff_c++] = {
			it->first.qid,
			it->first.d,
			it->second->relevance,
			it->second->clicks
		};
	qsort(
		result_buff,
		result_buff_c,
		sizeof(QueryDocResult),
		compare_query_doc_result
	);
	ui32 prev_query = 0;
	ui64 max_run = 0;
	ui64 current_run = 0;
	for (ui64 i = 0; i < result_buff_c; ++i) {
		if (prev_query != result_buff[i].qid) {
			prev_query = result_buff[i].qid;
			if (current_run > max_run) max_run = current_run;
			current_run = 0;
		}
		++current_run;
	}
	if (current_run > max_run) max_run = current_run;
	fwrite(&max_run, sizeof(ui64), 1, stdout);
	DocumentClicksRelevance *doc_rel_buffer =
		(DocumentClicksRelevance*)
			malloc(max_run * sizeof(DocumentClicksRelevance));
	ui64 doc_rel_c = 0;
	for (ui64 i = 0; i < result_buff_c; ++i) {
		if (result_buff[i].d == 0) continue;
		ui32 query = result_buff[i].qid;
		if (query != prev_query) {
			if (doc_rel_c > 0) {
				fwrite(&prev_query, sizeof(ui32), 1, stdout);
				fwrite(&doc_rel_c, sizeof(ui64), 1, stdout);
				for (ui64 j = 0; j < doc_rel_c; ++j) {
					ui32 d = doc_rel_buffer[j].d;
					probability relevance = doc_rel_buffer[j].relevance;
					ui32 clicks = doc_rel_buffer[j].clicks;
					fwrite(&d, sizeof(ui32), 1, stdout);
					fwrite(&clicks, sizeof(ui32), 1, stdout);
					fwrite(&relevance, sizeof(probability), 1, stdout);
				}
				doc_rel_c = 0;
			}
			prev_query = query;
		}
		doc_rel_buffer[doc_rel_c++] = {
			result_buff[i].d,
			result_buff[i].clicks,
			result_buff[i].relevance
		};
	}
	fwrite(&prev_query, sizeof(ui32), 1, stdout);
	fwrite(&doc_rel_c, sizeof(ui64), 1, stdout);
	for (ui64 j = 0; j < doc_rel_c; ++j) {
		ui32 d = doc_rel_buffer[j].d;
		probability relevance = doc_rel_buffer[j].relevance;
		ui32 clicks = doc_rel_buffer[j].clicks;
		fwrite(&d, sizeof(ui32), 1, stdout);
		fwrite(&clicks, sizeof(ui32), 1, stdout);
		fwrite(&relevance, sizeof(probability), 1, stdout);
	}
}
