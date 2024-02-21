#include <math.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <algorithm>
#include <unistd.h>
#include <unordered_map>
#include <unordered_set>

#include "strings.h"
#include "types.h"
#include "config.c"
#include "constants.h"
#include "attention.cpp"
#include "utils.c"
#include "metrics.cpp"
#include "help.c"
#include "debug.c"

std::unordered_map<ui64, Context> CANDIDATES;

void free_rank_contexts(Context ctx) {
	for (int i = 0; i < ctx.rank_contexts_c; i++) {
		free(ctx.rank_contexts[i].candidates_ptr);
	}
	free(ctx.rank_contexts);
	free(ctx.rank_contexts_per_rank);
}

void free_context(Context ctx) {
	free_rank_contexts(ctx);
	free(ctx.candidates_ptr);
}

void build_rank_candidate_lists(
	Context *ctx, f64 quality_threshold
) {
	ctx->rank_contexts =
		(RankContext*) malloc(PAGE_LENGTH * sizeof(RankContext));
	ctx->rank_contexts_per_rank =
		(RankContext**) malloc(PAGE_LENGTH * sizeof(RankContext*));
	ui64 offset = 0;
	ui64 previous_offset = 0;
	long last_candidate_list_length = -1;
	ui64 rank_contexts_c = 0;
	for (int j = 1; j <= PAGE_LENGTH; j++) {
		f64 min_rel_priori =
			min_rel_priori_dcg(ctx->candidates_ptr, j, quality_threshold);
		ui64 i;
		for (i = offset; i < ctx->candidates_c; i++)
			if (ctx->candidates_ptr[i].relevance < min_rel_priori) break;
		ui64 next_candidate_list_length = i - offset;
		if (
			last_candidate_list_length != -1
			&& next_candidate_list_length == last_candidate_list_length
			&& offset == previous_offset
		) {
			ctx->rank_contexts_per_rank[j - 1] =
				ctx->rank_contexts_per_rank[j - 2];
		} else {
			ctx->rank_contexts[rank_contexts_c].candidates_c =
				next_candidate_list_length;
			ui64 size = next_candidate_list_length;
			Candidate **candidates_ptr =
				(Candidate**) malloc(size * sizeof(Candidate*));
			ctx->rank_contexts[rank_contexts_c].candidates_ptr = candidates_ptr;
			for (ui64 i = 0; i < next_candidate_list_length; ++i) {
				ctx->rank_contexts[rank_contexts_c].candidates_ptr[i] =
					&ctx->candidates_ptr[i + offset];
			}
			std::sort(
				ctx->rank_contexts[rank_contexts_c].candidates_ptr,
				ctx->rank_contexts[rank_contexts_c].candidates_ptr +
					next_candidate_list_length,
				compare_candidate_ptr
			);
			rank_contexts_c++;
			ctx->rank_contexts_per_rank[j - 1] =
				&ctx->rank_contexts[rank_contexts_c - 1];
		}
		last_candidate_list_length = next_candidate_list_length;
		previous_offset = offset;
		if (i == offset + 1) offset = i;
	}
	ctx->rank_contexts_c = rank_contexts_c;
}

Context context_from_sorted_candidates(
	Candidate *candidates_ptr,
	ui64 len,
	f64 quality_threshold
) {
	Context ctx;
	ctx.candidates_c = len;
	ctx.candidates_ptr = (Candidate*) malloc(len * sizeof(Candidate));
	memcpy(
		ctx.candidates_ptr, candidates_ptr, len * sizeof(Candidate)
	);
	int cutoff = (PAGE_LENGTH < len ? PAGE_LENGTH : len);
	f64 idcg = 0;
	for (ui64 i = 0; i < cutoff; i++)
		idcg += discounted_gain(candidates_ptr[i].relevance, i + 1);
	ctx.idcg = idcg;
	ctx.quality_threshold = quality_threshold;
	build_rank_candidate_lists(&ctx, quality_threshold);
	return ctx;
}

void update_relevance(FILE *job_fp, FILE *job_end_fp) {
	ui64 query_id;
	if (fread(&query_id, sizeof(ui64), 1, job_fp) != 1)
		err_exit("error reading query_id\n");
	f64 quality_threshold;
	if (fread(&quality_threshold, sizeof(f64), 1, job_fp) != 1)
		err_exit("error reading quality_threshold\n");
	ui64 doc_id;
	f64 relevance;
	Candidate *doc_relevance_update = doc_relevance_update_buffer();
	ui64 row = 0;
	auto it = CANDIDATES.find(query_id);
	bool splicing = it != CANDIDATES.end();
	std::unordered_map<ui64, ui64> doc_id_to_exposure;
	Context prev_ctx;
	if (splicing) {
		prev_ctx = it->second;
		for (int i = 0; i < prev_ctx.candidates_c; i++) {
			doc_id_to_exposure[prev_ctx.candidates_ptr[i].doc_id] =
				prev_ctx.candidates_ptr[i].exposure;
		}
		free_context(prev_ctx);
	}
	while (fread(&doc_id, sizeof(ui64), 1, job_fp) == 1) {
		if (fread(&relevance, sizeof(f64), 1, job_fp) == 1) {
			doc_relevance_update[row].doc_id = doc_id;
			doc_relevance_update[row].relevance = relevance;
			if (splicing) {
				auto it = doc_id_to_exposure.find(doc_id);
				if (it != doc_id_to_exposure.end()) {
					doc_relevance_update[row].exposure = it->second;
				}
				else doc_relevance_update[row].exposure = 0;
			} else
				doc_relevance_update[row].exposure = 0;
			doc_relevance_update[row].masked = false;
			row++;
		} else {
			err_exit("error reading relevance (Y)\n");
		}
	}
	doc_id_to_exposure.clear();
	std::sort(
		doc_relevance_update,
		doc_relevance_update + row,
		compare_candidate_by_relevance
	);
	char *the_qrel_file = qrel_file(query_id);
	FILE *qrel_fp = safe_file_write(the_qrel_file);
	safe_write_item(&row, sizeof(ui64), qrel_fp);
	for (int i = 0; i < row; i++) {
		safe_write_item(
			&doc_relevance_update[i].doc_id, sizeof(ui64), qrel_fp);
		safe_write_item(
			&doc_relevance_update[i].relevance, sizeof(f64), qrel_fp);
		safe_write_item(
			&doc_relevance_update[i].exposure, sizeof(ui64), qrel_fp);
	}
	fclose(qrel_fp);
	Context ctx = context_from_sorted_candidates(
		doc_relevance_update, row, quality_threshold
	);
	CANDIDATES[query_id] = ctx;
}

f64 min_rank_rel(f64 min_dcg, ui64 rnk, f64 other_dcg_weight) {
	if (min_dcg <= other_dcg_weight) return 0;
	return log2(1 + log2(rnk + 1) * (min_dcg - other_dcg_weight));
}

f64 unfairness(Context ctx) {
	f64 total_relevance = 0;
	f64 total_exposure = 0;
	for (ui64 i = 0; i < ctx.candidates_c; i++) {
		total_relevance += ctx.candidates_ptr[i].relevance;
		total_exposure += ctx.candidates_ptr[i].exposure;
		Candidate candidate = ctx.candidates_ptr[i];
	}
	f64 unfairness = 0;
	for (ui64 i = 0; i < ctx.candidates_c; i++) {
		Candidate candidate = ctx.candidates_ptr[i];
		f64 relevance = candidate.relevance;
		f64 exposure = candidate.exposure;
		f64 relevance_ratio = relevance / total_relevance;
		f64 exposure_ratio = exposure / total_exposure;
		f64 diff = relevance_ratio - exposure_ratio;
		if (diff > 0) unfairness += diff;
	}
	return unfairness;
}

void gen_ranking(FILE *job_fp, FILE *job_end_fp) {
	ui64 query_id;
	if (fread(&query_id, sizeof(ui64), 1, job_fp) != 1)
		err_exit("error reading query_id\n");
	f64 quality_threshold;
	if (fread(&quality_threshold, sizeof(f64), 1, job_fp) != 1)
		err_exit("error reading quality_threshold\n");
	auto it = CANDIDATES.find(query_id);
	Context ctx;
	if (it == CANDIDATES.end()) {
		char *the_qrel_file = qrel_file(query_id);
		FILE *qrel_fp = safe_file_read(the_qrel_file);
		ui64 len;
		safe_read_item(&len, sizeof(ui64), qrel_fp);
		Candidate *candidates_ptr =
			(Candidate*) malloc(len * sizeof(Candidate));
		for (ui64 i = 0; i < len; i++) {
			ui64 doc_id;
			f64 relevance;
			ui64 exposure;
			safe_read_item(&doc_id, sizeof(ui64), qrel_fp);
			safe_read_item(&relevance, sizeof(f64), qrel_fp);
			safe_read_item(&exposure, sizeof(ui64), qrel_fp);
			candidates_ptr[i].doc_id = doc_id;
			candidates_ptr[i].relevance = relevance;
			candidates_ptr[i].exposure = exposure;
			candidates_ptr[i].masked = false;
		}
		fclose(qrel_fp);
		ctx = context_from_sorted_candidates(
			candidates_ptr, len, quality_threshold
		);
		CANDIDATES[query_id] = ctx;
	} else {
		ctx = it->second;
		if (ctx.quality_threshold != quality_threshold) {
			free_rank_contexts(ctx);
			build_rank_candidate_lists(&ctx, quality_threshold);
		}
		CANDIDATES[query_id] = ctx;
	}
	const ui64 local_page_length = MIN(ctx.candidates_c, PAGE_LENGTH);
	if (local_page_length == 0) return;
	f64 before_weight = 0;
	f64 min_rel = 0;
	ui64 rnk = 1;
	Candidate **ranking = ranking_buffer();
	// print all candidates IDs
	do {
		const RankContext *rc = ctx.rank_contexts_per_rank[rnk - 1];
		//print_rank_candidate_lists(ctx);
		Candidate **candidates_ptr = rc->candidates_ptr;
		Candidate *candidate;
		for (ui64 j = 1; j <= rc->candidates_c; j++) {
			candidate = candidates_ptr[j - 1];
			if (candidate->relevance >= min_rel && !candidate->masked) {
				break;
			}
		}
		safe_write_item(&candidate->doc_id, sizeof(ui64), job_end_fp);
		candidate->masked = true;
		before_weight += discounted_gain(candidate->relevance, rnk);
		ranking[rnk - 1] = candidate;
		rnk++;
		if (rnk <= local_page_length) {
			f64 last_weight = 9e99;
			Candidate* top_candidate = ctx.candidates_ptr;
			f64 optimistic_dcg_suf = 0;
			for (ui64 suf_rnk = rnk + 1; suf_rnk <= PAGE_LENGTH; ++suf_rnk) {
				while (top_candidate->masked) ++top_candidate;
				last_weight = discounted_gain(
					top_candidate->relevance,
					suf_rnk
				);
				optimistic_dcg_suf += last_weight;
				++top_candidate;
			}
			min_rel = min_rank_rel(
				ctx.idcg * ctx.quality_threshold,
				rnk,
				before_weight + optimistic_dcg_suf
			);
			if (min_rel >= last_weight) {
				Candidate min_rel_candidate = {0, min_rel, 0, false};
				Candidate *n_candidate = std::upper_bound(
					ctx.candidates_ptr,
					top_candidate,
					min_rel_candidate,
					compare_candidate_by_relevance
				) - 1;
				do {
					while (n_candidate->masked) --n_candidate;
					// Print relevance
					f64 optimistic_dcg_suf = 0;
					top_candidate = ctx.candidates_ptr;
					n_candidate->masked = true;
					for (ui64 j = rnk + 1; j <= PAGE_LENGTH; ++j) {
						while (top_candidate->masked) ++top_candidate;
						optimistic_dcg_suf += discounted_gain(
							top_candidate->relevance,
							j
						);
						++top_candidate;
					}
					n_candidate->masked = false;
					min_rel = min_rank_rel(
						ctx.idcg * ctx.quality_threshold,
						rnk,
						before_weight + optimistic_dcg_suf
					);
				} while ((n_candidate--)->relevance < min_rel);
			}
		}
	} while (rnk <= local_page_length);
	ui8 *attention = attention_buffer();
	geometric_attention(attention, 0.40);
	for (ui64 i = 0; i < local_page_length; i++) ranking[i]->masked = false;
	std::sort( ranking, ranking + local_page_length, compare_candidate_ptr);
	for (ui64 rnk = local_page_length; rnk >= 1; --rnk) {
		Candidate *update_me = ranking[rnk - 1];
		ui64 next_state_exposure = update_me->exposure + attention[rnk - 1];
		Candidate next_state = *update_me;
		next_state.exposure = next_state_exposure;
		for (ui64 i = 0; i < ctx.rank_contexts_c; ++i) {
			const RankContext rc = ctx.rank_contexts[i];
			Candidate last_item = ctx.candidates_ptr[rc.candidates_c - 1];
			if (update_me->relevance >= last_item.relevance) {
				Candidate **update_me_ptr = std::lower_bound(
					rc.candidates_ptr,
					rc.candidates_ptr + rc.candidates_c,
					update_me,
					compare_candidate_ptr
				);
				Candidate **move_here = std::lower_bound(
					rc.candidates_ptr,
					rc.candidates_ptr + rc.candidates_c,
					&next_state,
					compare_candidate_ptr
				) - 1;
				if (move_here > update_me_ptr) {
					ui64 distance = move_here - update_me_ptr;
					memmove(
						update_me_ptr,
						update_me_ptr + 1,
						distance * sizeof(Candidate*)
					);
					*move_here = update_me;
				}
			}
		}
		update_me->exposure = next_state_exposure;
	}
	f64 unfairness_score = unfairness(ctx);
	printf("unfairness,%f\n", unfairness_score);
	//print_rank_candidate_lists(ctx);
}

void get_candidates(FILE *job_fp, FILE *job_end_fp) {
	ui64 query_id;
	if (fread(&query_id, sizeof(ui64), 1, job_fp) != 1)
		err_exit("error reading query_id\n");
	f64 quality_threshold;
	if (fread(&quality_threshold, sizeof(f64), 1, job_fp) != 1)
		err_exit("error reading quality_threshold\n");
	auto it = CANDIDATES.find(query_id);
	Context ctx;
	if (it == CANDIDATES.end()) {
		char *the_qrel_file = qrel_file(query_id);
		FILE *qrel_fp = safe_file_read(the_qrel_file);
		ui64 len;
		safe_read_item(&len, sizeof(ui64), qrel_fp);
		Candidate *candidates_ptr =
			(Candidate*) malloc(len * sizeof(Candidate));
		for (ui64 i = 0; i < len; i++) {
			ui64 doc_id;
			f64 relevance;
			ui64 exposure;
			safe_read_item(&doc_id, sizeof(ui64), qrel_fp);
			safe_read_item(&relevance, sizeof(f64), qrel_fp);
			safe_read_item(&exposure, sizeof(ui64), qrel_fp);
			candidates_ptr[i].doc_id = doc_id;
			candidates_ptr[i].relevance = relevance;
			candidates_ptr[i].exposure = exposure;
			candidates_ptr[i].masked = false;
		}
		fclose(qrel_fp);
		ctx = context_from_sorted_candidates(
			candidates_ptr, len, quality_threshold
		);
		CANDIDATES[query_id] = ctx;
	} else {
		ctx = it->second;
		if (ctx.quality_threshold != quality_threshold) {
			free_rank_contexts(ctx);
			build_rank_candidate_lists(&ctx, quality_threshold);
		}
		CANDIDATES[query_id] = ctx;
	}
	const ui64 local_page_length = MIN(ctx.candidates_c, PAGE_LENGTH);
	if (local_page_length == 0) return;
	ui64 rnk = 1;
	Candidate **ranking = ranking_buffer();
	do {
		RankContext *rc = ctx.rank_contexts_per_rank[rnk - 1];
		Candidate **candidates_ptr = rc->candidates_ptr;
		Candidate *candidate;
		safe_write_item(&rc->candidates_c, sizeof(ui64), job_end_fp);
		for (ui64 j = 1; j <= rc->candidates_c; j++) {
			candidate = candidates_ptr[j - 1];
			safe_write_item(&candidate->doc_id, sizeof(ui64), job_end_fp);
			safe_write_item(&candidate->relevance, sizeof(f64), job_end_fp);
		}
		rnk++;
	} while (rnk <= local_page_length);
}

void launch_experiment(FILE *job_fp, FILE *job_end_fp) {
	ui64 query_id;
	if (fread(&query_id, sizeof(ui64), 1, job_fp) != 1)
		err_exit("error reading query_id\n");
	f64 quality_threshold;
	if (fread(&quality_threshold, sizeof(f64), 1, job_fp) != 1)
		err_exit("error reading quality_threshold\n");
	ui64 repetitions;
	if (fread(&repetitions, sizeof(ui64), 1, job_fp) != 1)
		err_exit("error reading repetitions\n");
	auto it = CANDIDATES.find(query_id);
	Context ctx;
	if (it == CANDIDATES.end()) {
		char *the_qrel_file = qrel_file(query_id);
		FILE *qrel_fp = safe_file_read(the_qrel_file);
		ui64 len;
		safe_read_item(&len, sizeof(ui64), qrel_fp);
		Candidate *candidates_ptr =
			(Candidate*) malloc(len * sizeof(Candidate));
		for (ui64 i = 0; i < len; i++) {
			ui64 doc_id;
			f64 relevance;
			ui64 exposure;
			safe_read_item(&doc_id, sizeof(ui64), qrel_fp);
			safe_read_item(&relevance, sizeof(f64), qrel_fp);
			safe_read_item(&exposure, sizeof(ui64), qrel_fp);
			candidates_ptr[i].doc_id = doc_id;
			candidates_ptr[i].relevance = relevance;
			candidates_ptr[i].exposure = exposure;
			candidates_ptr[i].masked = false;
		}
		fclose(qrel_fp);
		ctx = context_from_sorted_candidates(
			candidates_ptr, len, quality_threshold
		);
		CANDIDATES[query_id] = ctx;
	} else {
		ctx = it->second;
		if (ctx.quality_threshold != quality_threshold) {
			free_rank_contexts(ctx);
			build_rank_candidate_lists(&ctx, quality_threshold);
		}
		CANDIDATES[query_id] = ctx;
	}
	const ui64 local_page_length = MIN(ctx.candidates_c, PAGE_LENGTH);
	if (local_page_length == 0) return;
	for (ui64 rep = 1; rep <= repetitions; ++rep) {
		f64 before_weight = 0;
		f64 min_rel = 0;
		ui64 rnk = 1;
		Candidate **ranking = ranking_buffer();
		do {
			const RankContext *rc = ctx.rank_contexts_per_rank[rnk - 1];
			Candidate **candidates_ptr = rc->candidates_ptr;
			Candidate *candidate;
			for (ui64 j = 1; j <= rc->candidates_c; j++) {
				candidate = candidates_ptr[j - 1];
				if (candidate->relevance >= min_rel && !candidate->masked) {
					break;
				}
			}
			candidate->masked = true;
			before_weight += discounted_gain(candidate->relevance, rnk);
			ranking[rnk - 1] = candidate;
			rnk++;
			if (rnk <= local_page_length) {
				f64 last_weight = 9e99;
				Candidate* top_candidate = ctx.candidates_ptr;
				f64 optimistic_dcg_suf = 0;
				for (ui64 suf_rnk = rnk + 1; suf_rnk <= PAGE_LENGTH; ++suf_rnk) {
					while (top_candidate->masked) ++top_candidate;
					last_weight = discounted_gain(
						top_candidate->relevance,
						suf_rnk
					);
					optimistic_dcg_suf += last_weight;
					++top_candidate;
				}
				min_rel = min_rank_rel(
					ctx.idcg * ctx.quality_threshold,
					rnk,
					before_weight + optimistic_dcg_suf
				);
				if (min_rel >= last_weight) {
					Candidate min_rel_candidate = {0, min_rel, 0, false};
					Candidate *n_candidate = std::upper_bound(
						ctx.candidates_ptr,
						top_candidate,
						min_rel_candidate,
						compare_candidate_by_relevance
					) - 1;
					do {
						while (n_candidate->masked) --n_candidate;
						f64 optimistic_dcg_suf = 0;
						top_candidate = ctx.candidates_ptr;
						n_candidate->masked = true;
						for (ui64 j = rnk + 1; j <= PAGE_LENGTH; ++j) {
							while (top_candidate->masked) ++top_candidate;
							optimistic_dcg_suf += discounted_gain(
								top_candidate->relevance,
								j
							);
							++top_candidate;
						}
						n_candidate->masked = false;
						min_rel = min_rank_rel(
							ctx.idcg * ctx.quality_threshold,
							rnk,
							before_weight + optimistic_dcg_suf
						);
					} while ((n_candidate--)->relevance < min_rel);
				}
			}
		} while (rnk <= local_page_length);
		ui8 *attention = attention_buffer();
		geometric_attention(attention, 0.40);
		for (ui64 i = 0; i < local_page_length; i++) ranking[i]->masked = false;
		std::sort( ranking, ranking + local_page_length, compare_candidate_ptr);
		for (ui64 rnk = local_page_length; rnk >= 1; --rnk) {
			Candidate *update_me = ranking[rnk - 1];
			ui64 next_state_exposure = update_me->exposure + attention[rnk - 1];
			Candidate next_state = *update_me;
			next_state.exposure = next_state_exposure;
			for (ui64 i = 0; i < ctx.rank_contexts_c; ++i) {
				const RankContext rc = ctx.rank_contexts[i];
				Candidate last_item = ctx.candidates_ptr[rc.candidates_c - 1];
				if (update_me->relevance >= last_item.relevance) {
					Candidate **update_me_ptr = std::lower_bound(
						rc.candidates_ptr,
						rc.candidates_ptr + rc.candidates_c,
						update_me,
						compare_candidate_ptr
					);
					Candidate **move_here = std::lower_bound(
						rc.candidates_ptr,
						rc.candidates_ptr + rc.candidates_c,
						&next_state,
						compare_candidate_ptr
					) - 1;
					if (move_here > update_me_ptr) {
						ui64 distance = move_here - update_me_ptr;
						memmove(
							update_me_ptr,
							update_me_ptr + 1,
							distance * sizeof(Candidate*)
						);
						*move_here = update_me;
					}
				}
			}
			update_me->exposure = next_state_exposure;
		}
		f64 unfairness_score = unfairness(ctx);
		safe_write_item(&unfairness_score, sizeof(f64), job_end_fp);
	}
}

void set_job_path(PathBuffer buffer, ui64 job_id) {
	char *path = buffer.buffer;
	for (int i = 0; i < 16; i++) {
		char c = (char) ((job_id >> (i * 4)) & 0xF);
		path[buffer.length + i] = c < 10 ? c + '0' : c - 10 + 'a';
	}
}

void start_daemon() {
	do {
		FILE *job_queue_fp = safe_file_read(job_queue());
		ui64 job_id;
		PathBuffer the_job_start_file_buffer = job_start_file_buffer();
		PathBuffer the_job_end_fifo_buffer = job_end_fifo_buffer();
		int out = 0;
		while (
			fread(&job_id, sizeof(ui64), 1, job_queue_fp) == 1
		) {
			set_job_path(the_job_start_file_buffer, job_id);
			set_job_path(the_job_end_fifo_buffer, job_id);
			FILE *job_fp = safe_file_read(the_job_start_file_buffer.buffer);
			FILE *job_end_fp =
				safe_file_append(the_job_end_fifo_buffer.buffer);
			Job job_type;
			if (fread(&job_type, 1, 1, job_fp) != 1)
				err_exit("error reading job type\n");
			switch ((unsigned char) job_type) {
				case (unsigned char) Job::UPDATE_RELEVANCE:
					update_relevance(job_fp, job_end_fp);
					break;
				case (unsigned char) Job::GEN_RANKING:
					gen_ranking(job_fp, job_end_fp);
					break;
				case (unsigned char) Job::GET_CANDIDATES:
					get_candidates(job_fp, job_end_fp);
					break;
				case (unsigned char) Job::LAUNCH_EXPERIMENT:
					launch_experiment(job_fp, job_end_fp);
					break;
			}
			fclose(job_fp);
			safe_file_remove(the_job_start_file_buffer.buffer);
			fclose(job_end_fp);
			safe_file_remove(the_job_end_fifo_buffer.buffer);
		}
		fclose(job_queue_fp);
	} while (true);
}

void rerank(char *input_buffer, f64 quality_threshold) {
	char c;
	ui64 offset = 0;
	ui64 job_id = t();
	while ((c = getchar()) != EOF) {
		if (c == '\t') err_exit(
			"expected query ID alone on first input line, "
			"got multiple columns"
		);
		else if (c == '\n') break;
		input_buffer[offset++] = c;
	}
	input_buffer[offset] = '\0';
	ui64 qid = gen_id(input_buffer);
	std::unordered_set<ui64> doc_hashes;
	PathBuffer the_job_start_file_buffer = job_start_file_buffer();
	char *job_file = the_job_start_file_buffer.buffer;
	set_job_path(the_job_start_file_buffer, job_id);
	FILE *job_fp = init_job_file(job_file, Job::UPDATE_RELEVANCE);
	safe_write_item(&qid, sizeof(ui64), job_fp);
	safe_write_item(&quality_threshold, sizeof(f64), job_fp);
	while (c != EOF) {
		offset = 0;
		while ((c = getchar()) != EOF) {
			if (c == '\t') break;
			else if (c == '\n') err_exit(
				"too few columns: "
				"input should be two columns, tab-separated"
			);
			input_buffer[offset++] = c;
		}
		if (c == EOF) break;
		input_buffer[offset] = '\0';
		ui64 doc_id = gen_id(input_buffer);
		offset = 0;
		while ((c = getchar()) != EOF) {
			if (c == '\n') break;
			if (c == '\t') err_exit(
				"too many columns: "
				"input should be two columns, tab-separated"
			);
			input_buffer[offset++] = c;
		}
		input_buffer[offset] = '\0';
		f64 relevance = atof(input_buffer);
		safe_write_item(&doc_id, sizeof(ui64), job_fp);
		safe_write_item(&relevance, sizeof(f64), job_fp);
		if (doc_hashes.find(doc_id) != doc_hashes.end()) {
			fclose(job_fp);
			safe_file_remove(job_file);
			err_exit("duplicate doc_id");
		} else doc_hashes.insert(doc_id);
	}
	fclose(job_fp);
	FILE *job_queue_fp = safe_file_append(job_queue());
	PathBuffer the_job_end_fifo_buffer = job_end_fifo_buffer();
	FILE *job_end_fp;
	if (doc_hashes.size() == 0) {
		safe_file_remove(job_file);
	} else {
		doc_hashes.clear();
		set_job_path(the_job_end_fifo_buffer, job_id);
		if (mkfifo(the_job_end_fifo_buffer.buffer, 0600) == -1)
			err_exit("mkfifo");
		safe_write_item(&job_id, sizeof(ui64), job_queue_fp);
		fflush(job_queue_fp);
		job_end_fp = safe_file_read(the_job_end_fifo_buffer.buffer);
		fclose(job_end_fp);
	}
	ui64 new_job_id = t();
	set_job_path(the_job_start_file_buffer, new_job_id);
	set_job_path(the_job_end_fifo_buffer, new_job_id);
	job_fp = init_job_file(job_file, Job::GEN_RANKING);
	safe_write_item(&qid, sizeof(ui64), job_fp);
	safe_write_item(&quality_threshold, sizeof(f64), job_fp);
	fclose(job_fp);
	if (mkfifo(the_job_end_fifo_buffer.buffer, 0600) == -1) err_exit("mkfifo");
	safe_write_item(&new_job_id, sizeof(ui64), job_queue_fp);
	fflush(job_queue_fp);
	job_end_fp = safe_file_read(the_job_end_fifo_buffer.buffer);
	bool one_result_found = false;
	do {
		ui64 doc_id;
		if (fread(&doc_id, sizeof(ui64), 1, job_end_fp) != 1)
			break;
		one_result_found = true;
		get_orig_doc_id(doc_id, input_buffer);
		printf("%s\n", input_buffer);
	} while (true);
	if (!one_result_found) err_exit("qid results not loaded previously");
	fclose(job_end_fp);
	fclose(job_queue_fp);
	f64 relevance = atof(input_buffer);
}

void show_candidates(char *input_buffer, f64 quality_threshold) {
	char c;
	ui64 offset = 0;
	ui64 job_id = t();
	while ((c = getchar()) != EOF) {
		if (c == '\t') err_exit(
			"expected query ID alone on first input line, "
			"got multiple columns"
		);
		else if (c == '\n') break;
		input_buffer[offset++] = c;
	}
	input_buffer[offset] = '\0';
	ui64 qid = gen_id(input_buffer);
	std::unordered_set<ui64> doc_hashes;
	PathBuffer the_job_start_file_buffer = job_start_file_buffer();
	char *job_file = the_job_start_file_buffer.buffer;
	set_job_path(the_job_start_file_buffer, job_id);
	FILE *job_fp = init_job_file(job_file, Job::UPDATE_RELEVANCE);
	safe_write_item(&qid, sizeof(ui64), job_fp);
	safe_write_item(&quality_threshold, sizeof(f64), job_fp);
	while (c != EOF) {
		offset = 0;
		while ((c = getchar()) != EOF) {
			if (c == '\t') break;
			else if (c == '\n') err_exit(
				"too few columns: "
				"input should be two columns, tab-separated"
			);
			input_buffer[offset++] = c;
		}
		if (c == EOF) break;
		input_buffer[offset] = '\0';
		ui64 doc_id = gen_id(input_buffer);
		offset = 0;
		while ((c = getchar()) != EOF) {
			if (c == '\n') break;
			if (c == '\t') err_exit(
				"too many columns: "
				"input should be two columns, tab-separated"
			);
			input_buffer[offset++] = c;
		}
		input_buffer[offset] = '\0';
		f64 relevance = atof(input_buffer);
		safe_write_item(&doc_id, sizeof(ui64), job_fp);
		safe_write_item(&relevance, sizeof(f64), job_fp);
		if (doc_hashes.find(doc_id) != doc_hashes.end()) {
			fclose(job_fp);
			safe_file_remove(job_file);
			err_exit("duplicate doc_id");
		} else doc_hashes.insert(doc_id);
	}
	fclose(job_fp);
	FILE *job_queue_fp = safe_file_append(job_queue());
	PathBuffer the_job_end_fifo_buffer = job_end_fifo_buffer();
	FILE *job_end_fp;
	if (doc_hashes.size() == 0) {
		safe_file_remove(job_file);
	} else {
		doc_hashes.clear();
		set_job_path(the_job_end_fifo_buffer, job_id);
		if (mkfifo(the_job_end_fifo_buffer.buffer, 0600) == -1)
			err_exit("mkfifo");
		safe_write_item(&job_id, sizeof(ui64), job_queue_fp);
		fflush(job_queue_fp);
		job_end_fp = safe_file_read(the_job_end_fifo_buffer.buffer);
		fclose(job_end_fp);
	}
	ui64 new_job_id = t();
	set_job_path(the_job_start_file_buffer, new_job_id);
	set_job_path(the_job_end_fifo_buffer, new_job_id);
	job_fp = init_job_file(job_file, Job::GET_CANDIDATES);
	safe_write_item(&qid, sizeof(ui64), job_fp);
	safe_write_item(&quality_threshold, sizeof(f64), job_fp);
	fclose(job_fp);
	if (mkfifo(the_job_end_fifo_buffer.buffer, 0600) == -1) err_exit("mkfifo");
	safe_write_item(&new_job_id, sizeof(ui64), job_queue_fp);
	fflush(job_queue_fp);
	job_end_fp = safe_file_read(the_job_end_fifo_buffer.buffer);
	bool one_result_found = false;
	do {
		ui64 candidates_c;
		if (fread(&candidates_c, sizeof(ui64), 1, job_end_fp) != 1)
			break;
		for (ui64 i = 0; i < candidates_c; i++) {
			ui64 doc_id;
			f64 relevance;
			if (fread(&doc_id, sizeof(ui64), 1, job_end_fp) != 1)
				err_exit("error reading doc_id\n");
			if (fread(&relevance, sizeof(f64), 1, job_end_fp) != 1) {
				err_exit("error reading relevance (X)\n");
			}
			get_orig_doc_id(doc_id, input_buffer);
			if (i == 0) printf("%s\t", input_buffer);
			else printf("\t%s\t", input_buffer);
			compact_print_float(relevance);
		}
		printf("\n");
		one_result_found = true;
	} while (true);
	if (!one_result_found) err_exit("qid results not loaded previously");
	fclose(job_end_fp);
	fclose(job_queue_fp);
	f64 relevance = atof(input_buffer);
}

void start_experiment(
	char *input_buffer,
	f64 quality_threshold,
	ui64 repetitions
) {
	char c;
	ui64 offset = 0;
	ui64 job_id = t();
	while ((c = getchar()) != EOF) {
		if (c == '\t') err_exit(
			"expected query ID alone on first input line, "
			"got multiple columns"
		);
		else if (c == '\n') break;
		input_buffer[offset++] = c;
	}
	input_buffer[offset] = '\0';
	ui64 qid = gen_id(input_buffer);
	std::unordered_set<ui64> doc_hashes;
	PathBuffer the_job_start_file_buffer = job_start_file_buffer();
	char *job_file = the_job_start_file_buffer.buffer;
	set_job_path(the_job_start_file_buffer, job_id);
	FILE *job_fp = init_job_file(job_file, Job::UPDATE_RELEVANCE);
	safe_write_item(&qid, sizeof(ui64), job_fp);
	safe_write_item(&quality_threshold, sizeof(f64), job_fp);
	while (c != EOF) {
		offset = 0;
		while ((c = getchar()) != EOF) {
			if (c == '\t') break;
			else if (c == '\n') err_exit(
				"too few columns: "
				"input should be two columns, tab-separated"
			);
			input_buffer[offset++] = c;
		}
		if (c == EOF) break;
		input_buffer[offset] = '\0';
		ui64 doc_id = gen_id(input_buffer);
		offset = 0;
		while ((c = getchar()) != EOF) {
			if (c == '\n') break;
			if (c == '\t') err_exit(
				"too many columns: "
				"input should be two columns, tab-separated"
			);
			input_buffer[offset++] = c;
		}
		input_buffer[offset] = '\0';
		f64 relevance = atof(input_buffer);
		safe_write_item(&doc_id, sizeof(ui64), job_fp);
		safe_write_item(&relevance, sizeof(f64), job_fp);
		if (doc_hashes.find(doc_id) != doc_hashes.end()) {
			fclose(job_fp);
			safe_file_remove(job_file);
			err_exit("duplicate doc_id");
		} else doc_hashes.insert(doc_id);
	}
	fclose(job_fp);
	FILE *job_queue_fp = safe_file_append(job_queue());
	PathBuffer the_job_end_fifo_buffer = job_end_fifo_buffer();
	FILE *job_end_fp;
	if (doc_hashes.size() == 0) {
		safe_file_remove(job_file);
	} else {
		doc_hashes.clear();
		set_job_path(the_job_end_fifo_buffer, job_id);
		if (mkfifo(the_job_end_fifo_buffer.buffer, 0600) == -1)
			err_exit("mkfifo");
		safe_write_item(&job_id, sizeof(ui64), job_queue_fp);
		fflush(job_queue_fp);
		job_end_fp = safe_file_read(the_job_end_fifo_buffer.buffer);
		fclose(job_end_fp);
	}
	ui64 new_job_id = t();
	set_job_path(the_job_start_file_buffer, new_job_id);
	set_job_path(the_job_end_fifo_buffer, new_job_id);
	job_fp = init_job_file(job_file, Job::LAUNCH_EXPERIMENT);
	safe_write_item(&qid, sizeof(ui64), job_fp);
	safe_write_item(&quality_threshold, sizeof(f64), job_fp);
	safe_write_item(&repetitions, sizeof(ui64), job_fp);
	fclose(job_fp);
	if (mkfifo(the_job_end_fifo_buffer.buffer, 0600) == -1) err_exit("mkfifo");
	safe_write_item(&new_job_id, sizeof(ui64), job_queue_fp);
	fflush(job_queue_fp);
	job_end_fp = safe_file_read(the_job_end_fifo_buffer.buffer);
	get_orig_doc_id(qid, input_buffer);
	for (ui64 rep = 1; rep <= repetitions; ++rep) {
		f64 unfairness;
		if (fread(&unfairness, sizeof(f64), 1, job_end_fp) != 1)
			err_exit("error reading unfairness\n");

		printf("%s\t%lf\t%lu\t%lf\n", input_buffer, quality_threshold, rep, unfairness);
	}
	fclose(job_end_fp);
	fclose(job_queue_fp);
}

int main(int argc, char *argv[]) {
	char *EXECUTABLE_NAME = argv[0];
	argv++;
	argc--;
	Subcommand subcommand;
	bool input_pipe_open = !isatty(fileno(stdin));
	char *input_buffer = (char*) malloc(INPUT_BUFFER_SIZE);
	if (argc == 0) subcommand = input_pipe_open ? RERANK : HELP;
	else if (strcmp(*argv, "help") == 0) subcommand = HELP;
	else if (strcmp(*argv, "rerank") == 0) subcommand = RERANK;
	else if (strcmp(*argv, "candidates") == 0) subcommand = SHOW_CANDIDATES;
	else if (strcmp(*argv, "daemon") == 0) subcommand = DAEMON;
	else if (strcmp(*argv, "experiment") == 0) subcommand = EXPERIMENT;
	else {
		argv--;
		argc++;
		subcommand = RERANK;
	}
	argv++;
	argc--;
	bool extra_args = argc > 0;
	f64 quality_threshold;
	switch (subcommand) {
		case HELP:
			if (!extra_args && !input_pipe_open)
				print_general_help_then_halt();
			if (extra_args && !input_pipe_open) {
				subcommand = strcmp(*argv, "rerank") == 0 ? RERANK :
					strcmp(*argv, "daemon") == 0 ? DAEMON :
					strcmp(*argv, "help") == 0 ? HELP : BAD_SUBCOMMAND;
				print_subcommand_help_then_halt(subcommand, 0);
			}
			if (!extra_args && input_pipe_open) print_data_help_then_halt();
			print_general_help_then_halt();
			break;
		case RERANK:
			if (!input_pipe_open)
				print_subcommand_help_then_halt(RERANK, 1);
			if (extra_args) quality_threshold = atof(*argv);
			else quality_threshold = 0.99;
			rerank(input_buffer, quality_threshold);
			break;
		case SHOW_CANDIDATES:
			if (!input_pipe_open)
				print_subcommand_help_then_halt(RERANK, 1);
			if (extra_args) quality_threshold = atof(*argv);
			else quality_threshold = 0.99;
			show_candidates(input_buffer, quality_threshold);
			break;
		case DAEMON:
			if (extra_args || input_pipe_open)
				print_subcommand_help_then_halt(DAEMON, 1);
			start_daemon();
			break;
		case EXPERIMENT:
			if (!extra_args || !input_pipe_open)
				print_subcommand_help_then_halt(EXPERIMENT, 1);
			quality_threshold = atof(*argv);
			argv++;
			ui64 repetitions;
			repetitions = atoll(*argv);
			start_experiment(input_buffer, quality_threshold, repetitions);
			break;
		default:
			print_general_help_then_halt();
	}
}
