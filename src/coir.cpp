#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <algorithm>
#include <unistd.h>
#include <unordered_set>

#include "types.h"

std::unordered_map<docid, gid> DOC_GROUPS;
qid CURRENT_QUERY;

#include "strings.h"
#include "config.c"
#include "constants.h"
#include "attention.cpp"
#include "errors.c"
#include "fp.c"
#include "io.c"
#include "utils.c"
#include "metrics.cpp"
#include "help.c"
#include "gc.c"
#include "debug.c"
#include "subcommands/convert_click_log.c"
#include "subcommands/read_sparse_click_log.c"
#include "subcommands/read_dense_click_log.c"
#include "subcommands/click_log_probable_rank_docs.cpp"
#include "subcommands/click_log_predict_rankings.cpp"
#include "subcommands/pscm.cpp"
#include "subcommands/read_config.c"
#include "subcommands/read_relevancy_file.c"
#include "subcommands/convert_dense_click_log.c"

std::unordered_map<ui64, Context> CANDIDATES;

void set_job_path(PathBuffer path_buffer) {
	for (int i = 0; i < 16; i++) {
		char c = (char) ((CURRENT_JOB >> (i * 4)) & 0xF);
		path_buffer.buffer[path_buffer.length + i] =
			c < 10 ? c + '0' : c - 10 + 'a';
	}
}

void set_start_job_path() {
	set_job_path(job_start_path_buffer());
}

void set_end_job_path() {
	set_job_path(job_end_fifo_buffer());
}

void set_query(qid query) {
	CURRENT_QUERY = query;
}

void new_job() {
	if (JOB_FILE != NULL) {
		fclose(JOB_FILE);
		JOB_FILE = NULL;
	}
	if (JOB_END_FILE != NULL)
		JOB_END_FILE = NULL;
	CURRENT_JOB = t();
	set_start_job_path();
	set_end_job_path();
}


void build_rank_candidate_lists(Context &ctx, f64 quality_threshold) {
	ctx.quality_threshold = quality_threshold;
	ctx.rank_contexts =
		(CandidateList*) malloc(PAGE_LENGTH * sizeof(CandidateList));
	ctx.rank_contexts_per_rank =
		(CandidateList**) malloc(PAGE_LENGTH * sizeof(CandidateList*));
	ui64 offset = 0;
	ui64 previous_offset = 0;
	long last_candidate_list_length = -1;
	ui64 rank_contexts_c = 0;
	for (ui64 rnk = 1; rnk <= PAGE_LENGTH; rnk++) {
		f64 min_rel_priori =
			min_rel_priori_dcg(ctx.candidates_ptr, rnk, quality_threshold);
		ui64 i;
		for (i = offset; i < ctx.candidates_c; i++)
			if (ctx.candidates_ptr[i].relevance < min_rel_priori) break;
		ui64 next_candidate_list_length = i - offset;
		if (
			last_candidate_list_length != -1
			&& next_candidate_list_length == last_candidate_list_length
			&& offset == previous_offset
		)
			ctx.rank_contexts_per_rank[rnk - 1] =
				ctx.rank_contexts_per_rank[rnk - 2];
		else {
			ctx.rank_contexts[rank_contexts_c].candidates_c =
				next_candidate_list_length;
			ui64 size = next_candidate_list_length;
			Candidate **candidates_ptr =
				(Candidate**) malloc(size * sizeof(Candidate*));
			ctx.rank_contexts[rank_contexts_c].candidates_ptr =
				candidates_ptr;
			for (ui64 i = 0; i < next_candidate_list_length; ++i)
				ctx.rank_contexts[rank_contexts_c].candidates_ptr[i] =
					&ctx.candidates_ptr[i + offset];
			std::sort(
				ctx.rank_contexts[rank_contexts_c].candidates_ptr,
				ctx.rank_contexts[rank_contexts_c].candidates_ptr +
					next_candidate_list_length,
				compare_candidate_ptr
			);
			rank_contexts_c++;
			ctx.rank_contexts_per_rank[rnk - 1] =
				&ctx.rank_contexts[rank_contexts_c - 1];
		}
		last_candidate_list_length = next_candidate_list_length;
		previous_offset = offset;
		if (i == offset + 1) offset = i;
	}
	ctx.rank_contexts_c = rank_contexts_c;
}

Context context_from_sorted_candidates(
	Candidate *candidates_ptr,
	ui64 len,
	f64 quality_threshold
) {
	Context ctx;
	ctx.candidates_c = len;
	ctx.candidates_ptr = (Candidate*) malloc(len * sizeof(Candidate));
	ctx.group_contexts = (GroupContext*) malloc(256 * sizeof(GroupContext));
	ctx.groups_c = 0;
	ctx.majority_group = &ctx.group_contexts[0];
	for (ui16 i = 0; i < 256; i++) {
		ctx.group_contexts[i].candidates.candidates_c = 0;
		ctx.group_contexts[i].exposure = 0;
		ctx.group_contexts[i].bulk_relevance = 0;
		ctx.group_contexts[i].offset = 0;
	}
	for (ui64 i = 0; i < len; i++) {
		ctx.candidates_ptr[i] = candidates_ptr[i];
		gid group = DOC_GROUPS[candidates_ptr[i].doc];
		if (group >= ctx.groups_c) ctx.groups_c = group + 1;
		ctx.group_contexts[group].candidates.candidates_c++;
		ctx.group_contexts[group].bulk_relevance
			+= candidates_ptr[i].relevance;
	}
	for (ui16 i = 0; i < 256; i++) {
		if (ctx.group_contexts[i].candidates.candidates_c > 0) {
			ctx.group_contexts[i].candidates.candidates_ptr =
				(Candidate**) malloc(
					ctx.group_contexts[i].candidates.candidates_c
					* sizeof(Candidate*)
				);
			ctx.group_contexts[i].candidates.candidates_c = 0;
		}
	}
	for (ui64 i = 0; i < len; i++) {
		gid group = DOC_GROUPS[candidates_ptr[i].doc];
		GroupContext group_context = ctx.group_contexts[group];
		ctx.group_contexts[group].candidates.candidates_ptr[
			ctx.group_contexts[group].candidates.candidates_c++
		] = &ctx.candidates_ptr[i];
	}
	memcpy(
		ctx.candidates_ptr, candidates_ptr, len * sizeof(Candidate)
	);
	int cutoff = (PAGE_LENGTH < len ? PAGE_LENGTH : len);
	f64 idcg = 0;
	for (ui64 i = 0; i < cutoff; i++)
		idcg += discounted_gain(candidates_ptr[i].relevance, i + 1);
	ctx.idcg = idcg;
	ctx.quality_threshold = quality_threshold;
	build_rank_candidate_lists(ctx, quality_threshold);
	return ctx;
}

void update_relevance() {
	set_query(qid_from_client());
	f64 quality_threshold = quality_threshold_from_client();
	Candidate *doc_relevance_update = doc_relevance_update_buffer();
	ui64 row = 0;
	auto it = CANDIDATES.find(CURRENT_QUERY);
	std::unordered_map<ui64, ui64> doc_id_to_exposure;
	Context prev_ctx;
	bool splicing = it != CANDIDATES.end();
	if (splicing) {
		prev_ctx = it->second;
		for (int i = 0; i < prev_ctx.candidates_c; i++)
			doc_id_to_exposure[prev_ctx.candidates_ptr[i].doc] =
					prev_ctx.candidates_ptr[i].exposure;
		free_context(prev_ctx);
	}
	docid doc;
	while (doc_from_client(doc)) {
		doc_relevance_update[row].doc = doc;
		doc_relevance_update[row].relevance = relevance_from_client();
		if (splicing) {
			auto it = doc_id_to_exposure.find(doc);
			if (it != doc_id_to_exposure.end())
				doc_relevance_update[row].exposure = it->second;
			else doc_relevance_update[row].exposure = 0;
		} else doc_relevance_update[row].exposure = 0;
		doc_relevance_update[row].masked = false;
		row++;
		DOC_GROUPS[doc] = group_from_client();
	}
	doc_id_to_exposure.clear();
	std::sort(
		doc_relevance_update,
		doc_relevance_update + row,
		compare_candidate_by_relevance
	);
	len_to_qrel(row);
	for (int i = 0; i < row; i++)
		candidate_to_qrel(doc_relevance_update[i]);
	close_qrel_file();
	Context ctx = context_from_sorted_candidates(
		doc_relevance_update, row, quality_threshold
	);
	if (splicing)
		for (ui16 i = 0; i < 256; i++)
			ctx.group_contexts[i].exposure =
				prev_ctx.group_contexts[i].exposure;
	CANDIDATES[CURRENT_QUERY] = ctx;
}

f64 min_rank_rel(f64 min_dcg, ui64 rnk, f64 other_dcg_weight) {
	if (min_dcg <= other_dcg_weight) return 0;
	return log2(1 + log2(rnk + 1) * (min_dcg - other_dcg_weight));
}

f64 individual_unfairness(Context ctx) {
	f64 total_relevance = 0;
	f64 total_exposure = 0;
	for (ui64 i = 0; i < ctx.candidates_c; i++) {
		total_relevance += ctx.candidates_ptr[i].relevance;
		total_exposure += ctx.candidates_ptr[i].exposure;
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

f64 group_unfairness(Context ctx) {
	f64 total_relevance = 0;
	f64 total_exposure = 0;
	for (gid g = 0; g < ctx.groups_c; ++g) {
		total_relevance += ctx.group_contexts[g].bulk_relevance;
		total_exposure += ctx.group_contexts[g].exposure;
	}
	f64 mean_relative_compensation = total_exposure / total_relevance;
	f64 total_unfairness = 0;
	ui8 underexposed_groups = 0;
	for (gid g = 0; g < ctx.groups_c; ++g) {
		if (ctx.group_contexts[g].candidates.candidates_c == 0) continue;
		f64 relevance = ctx.group_contexts[g].bulk_relevance;
		f64 exposure = ctx.group_contexts[g].exposure;
		f64 relative_compensation = exposure / relevance;
		if (mean_relative_compensation > relative_compensation) {
			f64 diff = mean_relative_compensation - relative_compensation;
			f64 error = 1 - relative_compensation / mean_relative_compensation;
			total_unfairness += error;
			++underexposed_groups;
		}
	}
	return total_unfairness / underexposed_groups;
}

Context get_context(f64 quality_threshold) {
	auto it = CANDIDATES.find(CURRENT_QUERY);
	Context ctx;
	if (it == CANDIDATES.end()) {
		ui64 len = len_from_qrel();
		Candidate *candidates_ptr =
	   		 (Candidate*) malloc(len * sizeof(Candidate));
		for (ui64 i = 0; i < len; i++) {
			candidates_ptr[i].doc = doc_from_qrel();
			candidates_ptr[i].relevance = relevance_from_qrel();
			candidates_ptr[i].exposure = exposure_from_qrel();
			candidates_ptr[i].masked = false;
			DOC_GROUPS[candidates_ptr[i].doc] = group_from_qrel();
		}
		close_qrel_file();
		ctx = context_from_sorted_candidates(
	   		 candidates_ptr, len, quality_threshold
		);
		CANDIDATES[CURRENT_QUERY] = ctx;
	} else {
		ctx = it->second;
		if (ctx.quality_threshold != quality_threshold) {
			free_rank_contexts(ctx);
			build_rank_candidate_lists(ctx, quality_threshold);
		}
		CANDIDATES[CURRENT_QUERY] = ctx;
	}
	return ctx;
}

void get_candidates() {
	set_query(qid_from_client());
	f64 quality_threshold = quality_threshold_from_client();
	Context ctx = get_context(quality_threshold);
	const ui64 local_page_length = MIN(ctx.candidates_c, PAGE_LENGTH);
	if (local_page_length == 0) return;
	ui64 rnk = 1;
	CandidateList last_rank_context =
		ctx.rank_contexts[ctx.rank_contexts_c - 1];
	ui64_to_client(last_rank_context.candidates_c);
	for (ui64 i = 0; i < last_rank_context.candidates_c; ++i) {
		Candidate *candidate = last_rank_context.candidates_ptr[i];
		doc_to_client(candidate->doc);
		relevance_to_client(candidate->relevance);
	}
	do {
		CandidateList *rc = ctx.rank_contexts_per_rank[rnk - 1];
		ui64_to_client(rc->candidates_c);
		for (ui64 j = 1; j <= rc->candidates_c; j++)
			doc_to_client(rc->candidates_ptr[j - 1]->doc);
		rnk++;
	} while (rnk <= local_page_length);
}

void update_exposure(
	Context ctx, RankedItem* ranking, ui64 local_page_length
) {
	std::sort(ranking, ranking + local_page_length, compare_ranked_items);
	ui8 *attention = attention_buffer();
	geometric_attention(attention, 0.40);
	f64 majority_relative_compensation =
		ctx.majority_group->exposure / ctx.majority_group->bulk_relevance;
	gid majority_group = ctx.majority_group - ctx.group_contexts;
	for (ui64 i = 0; i < local_page_length; ++i) {
		RankedItem ranked_item = ranking[i];
		Candidate *update_me = ranked_item.candidate;
		ui64 rnk = ranked_item.rank;
		ui8 item_attention = attention[rnk - 1];
		gid g = DOC_GROUPS[update_me->doc];
		ctx.group_contexts[g].exposure += item_attention;
		if (g == majority_group)
			majority_relative_compensation =
				ctx.majority_group->exposure
				/ ctx.majority_group->bulk_relevance;
		else {
			f64 relative_compensation =
				ctx.group_contexts[g].exposure
				/ ctx.group_contexts[g].bulk_relevance;
			if (relative_compensation > majority_relative_compensation) {
				ctx.majority_group = &ctx.group_contexts[g];
				majority_relative_compensation = relative_compensation;
			}
		}
		ui64 next_state_exposure = update_me->exposure + item_attention;
		Candidate next_state = *update_me;
		next_state.exposure = next_state_exposure;
		for (ui64 i = 0; i < ctx.rank_contexts_c; ++i) {
			const CandidateList rc = ctx.rank_contexts[i];
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
	for (ui16 g = 0; g < ctx.groups_c; g++) {
		if (ctx.group_contexts[g].candidates.candidates_c == 0) continue;
		f64 relative_compensation =
			ctx.group_contexts[g].exposure
			/ ctx.group_contexts[g].bulk_relevance;
		f64 error =
			(majority_relative_compensation - relative_compensation) / 255;
		ctx.group_contexts[g].error = error;
	}
}


void gen_fairco_re_ranking_inner(
	Context ctx,
	const ui64 local_page_length,
	RankingMode mode
) {
	RankedItem *ranking = ranking_buffer();
	ui64 rnk = 1;
	for (ui64 rnk = 1; rnk <= local_page_length; ++rnk) {
		Candidate *best = NULL;
		f64 best_score = 0;
		gid best_group = 0;
		for (ui16 g = 0; g < ctx.groups_c; g++) {
			GroupContext group_context = ctx.group_contexts[g];
			if (group_context.candidates.candidates_c == 0) continue;
			Candidate *candidate = group_context.candidates.candidates_ptr[
				group_context.offset];
			f64 score = candidate->relevance
				+ FAIRCO_LAMBDA * group_context.error;
			if (score > best_score) {
				best_score = candidate->relevance;
				best_group = g;
				best = candidate;
			}
		}
		ctx.group_contexts[best_group].offset++;
		ranking[rnk - 1] = {rnk, best};
		if (mode == STANDALONE) doc_to_client(best->doc);
	}
	for (ui16 g = 0; g < ctx.groups_c; g++)
		ctx.group_contexts[g].offset = 0;
	update_exposure(ctx, ranking, local_page_length);
	if (mode == PART_OF_EXPERIMENT) {
		unfairness_to_client(group_unfairness(ctx));
		unfairness_to_client(individual_unfairness(ctx));
	}
}

void gen_iaf_re_ranking_inner(
	f64 quality_threshold,
	Context ctx,
	const ui64 local_page_length,
	RankingMode mode
) {
	f64 before_weight = 0;
	f64 min_rel = 0;
	ui64 rnk = 1;
	RankedItem *ranking = ranking_buffer();
	do {
		const CandidateList *rc = ctx.rank_contexts_per_rank[rnk - 1];
		Candidate **candidates_ptr = rc->candidates_ptr;
		Candidate *candidate;
		for (ui64 j = 1; j <= rc->candidates_c; j++) {
			candidate = candidates_ptr[j - 1];
			if (candidate->relevance >= min_rel && !candidate->masked)
				break;
		}
		if (mode == STANDALONE)
			doc_to_client(candidate->doc);
		get_orig_doc_id(candidate->doc);
		candidate->masked = true;
		before_weight += discounted_gain(candidate->relevance, rnk);
		ranking[rnk - 1] = {rnk, candidate};
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
	for (ui64 i = 0; i < local_page_length; i++)
		ranking[i].candidate->masked = false;
	update_exposure(ctx, ranking, local_page_length);
	if (mode == PART_OF_EXPERIMENT) {
		unfairness_to_client(group_unfairness(ctx));
		unfairness_to_client(individual_unfairness(ctx));
	}
}

void gen_fairco_re_ranking() {
	set_query(qid_from_client());
	Context ctx = get_context(0);
	const ui64 local_page_length = MIN(ctx.candidates_c, PAGE_LENGTH);
	if (local_page_length == 0) return;
	gen_fairco_re_ranking_inner(ctx, local_page_length, STANDALONE);
}

void gen_iaf_re_ranking() {
	set_query(qid_from_client());
	f64 quality_threshold = quality_threshold_from_client();
	Context ctx = get_context(quality_threshold);
	const ui64 local_page_length = MIN(ctx.candidates_c, PAGE_LENGTH);
	if (local_page_length == 0) return;
	gen_iaf_re_ranking_inner(
		quality_threshold, ctx, local_page_length, STANDALONE
	);
}

void fairco_experiment() {
	set_query(qid_from_client());
	ui64 repetitions = repetitions_from_client();
	Context ctx = get_context(0);
	const ui64 local_page_length = MIN(ctx.candidates_c, PAGE_LENGTH);
	if (local_page_length == 0) return;
	for (ui64 rep = 1; rep <= repetitions; ++rep) gen_fairco_re_ranking_inner(
		ctx,
		local_page_length,
		PART_OF_EXPERIMENT
	);
}


void iaf_experiment() {
	set_query(qid_from_client());
	f64 quality_threshold = quality_threshold_from_client();
	ui64 repetitions = repetitions_from_client();
	Context ctx = get_context(quality_threshold);
	const ui64 local_page_length = MIN(ctx.candidates_c, PAGE_LENGTH);
	if (local_page_length == 0) return;
	for (ui64 rep = 1; rep <= repetitions; ++rep) {
		gen_iaf_re_ranking_inner(
			quality_threshold,
			ctx,
			local_page_length,
			PART_OF_EXPERIMENT
		);
	}
}

void start_daemon() {
	do {
		FILE *job_queue_fp = safe_file_read(job_queue());
		ui64 job_id;
		while (fread(&job_id, sizeof(ui64), 1, job_queue_fp) == 1) {
			CURRENT_JOB = job_id;
			set_start_job_path();
			set_end_job_path();
			ui8 job_type_ui8;
			if (fread(&job_type_ui8, 1, 1, read_job_file()) != 1)
				err_exit("error reading job type\n");
			Job job_type = (Job) job_type_ui8;
			switch (job_type) {
				case Job::UPDATE_RELEVANCE:
					update_relevance();
					break;
				case Job::GEN_RANKING:
					gen_iaf_re_ranking();
					break;
				case Job::GEN_FAIRCO_RE_RANKING:
					gen_fairco_re_ranking();
					break;
				case Job::GET_CANDIDATES:
					get_candidates();
					break;
				case Job::LAUNCH_IAF_EXPERIMENT:
					iaf_experiment();
					break;
				case Job::LAUNCH_FAIRCO_EXPERIMENT:
					fairco_experiment();
					break;
				case Job::STOP_DAEMON:
					exit(0);
			}
			fclose(JOB_FILE);
			JOB_FILE = NULL;
			safe_file_remove(job_start_path_buffer().buffer);
			fclose(write_job_end_file());
			JOB_END_FILE = NULL;
			safe_file_remove(job_end_fifo_buffer().buffer);
		}
		fclose(job_queue_fp);
	} while (true);
}

void queue_job() {
	if (JOB_FILE != NULL) {
		fclose(JOB_FILE);
		JOB_FILE = NULL;
	}
	if (mkfifo(job_end_fifo_buffer().buffer, 0600) == -1)
		err_exit("mkfifo");
	safe_write_item(&CURRENT_JOB, sizeof(job_id), append_queue());
	fflush(stdout);
	fflush(append_queue());
	read_job_end_file();
}

docid docid_from_stdin(char &c) {
	ui64 offset = 0;
	char *focus_buffer = get_focus_buffer();
	while (true) {
		c = getchar();
		if (c == '\t') break;
		if (c == '\n') break;
		if (c == EOF) break;
		focus_buffer[offset++] = c;
	}
	focus_buffer[offset] = '\0';
	return gen_id();
}

f64 relevance_from_stdin(char &c) {
	ui64 offset = 0;
	char *focus_buffer = get_focus_buffer();
	while (true) {
		c = getchar();
		if (c == '\t') break;
		if (c == '\n') break;
		if (c == EOF) break;
		focus_buffer[offset++] = c;
	}
	focus_buffer[offset] = '\0';
	return atof(focus_buffer);
}

gid group_from_stdin(char &c) {
	ui64 offset = 0;
	char *focus_buffer = get_focus_buffer();
	while (true) {
		c = getchar();
		if (c == '\t') break;
		if (c == '\n') break;
		if (c == EOF) break;
		focus_buffer[offset++] = c;
	}
	focus_buffer[offset] = '\0';
	return atoi(focus_buffer);
}

ui64 client_update_relevance(f64 quality_threshold) {
	new_job();
	char c;
	qid query = qid_from_stdin(c);
	if (c == '\t') expected_qid_alone_err();
	std::unordered_set<ui64> doc_hashes;
	init_job_file(Job::UPDATE_RELEVANCE);
	qid_to_daemon(query);
	quality_threshold_to_daemon(quality_threshold);
	while (c != EOF) {
		docid doc = docid_from_stdin(c);
		if (c == '\n') too_few_columns_err();
		if (c == EOF) break;
		f64 relevance = relevance_from_stdin(c);
		gid group = c == '\t' ? group_from_stdin(c) : NO_GROUP;
		doc_to_daemon(doc);
		relevance_to_daemon(relevance);
		group_to_daemon(group);
		if (doc_hashes.find(doc) != doc_hashes.end()) {
			doc_hashes.clear();
			cancel_job();
			duplicate_doc_id_err();
		} else doc_hashes.insert(doc);
	}
	if (doc_hashes.size() == 0)
		safe_file_remove(job_start_path_buffer().buffer);
	else {
		doc_hashes.clear();
		queue_job();
	}
	return query;
}

void iaf_rerank(f64 quality_threshold) {
	qid query = client_update_relevance(quality_threshold);
	new_job();
	init_job_file(Job::GEN_RANKING);
	qid_to_daemon(query);
	quality_threshold_to_daemon(quality_threshold);
	queue_job();
	bool one_result_found = false;
	do {
		docid doc;
		if (!doc_from_daemon(doc)) break;
		one_result_found = true;
		get_orig_doc_id(doc);
		printf("%s\n", get_focus_buffer());
	} while (true);
	if (!one_result_found) qid_unknown_err();
}

void fairco_rerank() {
	qid query = client_update_relevance(0);
	new_job();
	init_job_file(Job::GEN_FAIRCO_RE_RANKING);
	safe_write_item(&query, sizeof(qid), write_job_file());
	queue_job();
	bool one_result_found = false;
	do {
		docid doc;
		if (fread(&doc, sizeof(docid), 1, read_job_end_file()) != 1)
			break;
		one_result_found = true;
		get_orig_doc_id(doc);
		printf("%s\n", get_focus_buffer());
	} while (true);
	if (!one_result_found) qid_unknown_err();
}

void show_candidates(f64 quality_threshold) {
	qid query = client_update_relevance(quality_threshold);
	new_job();
	init_job_file(Job::GET_CANDIDATES);
	qid_to_daemon(query);
	quality_threshold_to_daemon(quality_threshold);
	queue_job();
	bool one_result_found = false;
	ui64 valid_docs;
	if (!ui64_from_daemon(valid_docs)) err_exit("error reading valid docs\n");
	for (ui64 i = 0; i < valid_docs; i++) {
		docid doc;
		if (!doc_from_daemon(doc)) err_exit("error reading doc\n");
		f64 relevance = relevance_from_daemon();
		get_orig_doc_id(doc);
		if (i == 0) printf("%s", get_focus_buffer());
		else printf("\t%s", get_focus_buffer());
		printf("\t");
		compact_print_float(relevance, 20);
	}
	printf("\n");
	do {
		ui64 candidates_c;
		if (!ui64_from_daemon(candidates_c)) break;
		for (ui64 i = 0; i < candidates_c; i++) {
			docid doc;
			if (!doc_from_daemon(doc)) err_exit("error reading doc\n");
			get_orig_doc_id(doc);
			if (i == 0) printf("%s", get_focus_buffer());
			else printf("\t%s", get_focus_buffer());
		}
		printf("\n");
		one_result_found = true;
	} while (true);
	if (!one_result_found) qid_unknown_err();
}

void iaf_experiment(f64 quality_threshold, ui64 repetitions) {
	qid query = client_update_relevance(quality_threshold);
	new_job();
	init_job_file(Job::LAUNCH_IAF_EXPERIMENT);
	qid_to_daemon(query);
	quality_threshold_to_daemon(quality_threshold);
	repetitions_to_daemon(repetitions);
	queue_job();
	get_orig_doc_id(query);
	for (ui64 rep = 1; rep <= repetitions; ++rep)
		printf(
			"%s\t%lf\t%lu\t%lf\t%lf\n",
			get_focus_buffer(),
			quality_threshold,
			rep,
			unfairness_from_daemon(),
			unfairness_from_daemon()
		);
}

void fairco_experiment(ui64 repetitions) {
	qid query = client_update_relevance(0);
	new_job();
	init_job_file(Job::LAUNCH_FAIRCO_EXPERIMENT);
	qid_to_daemon(query);
	repetitions_to_daemon(repetitions);
	queue_job();
	get_orig_doc_id(query);
	for (ui64 rep = 1; rep <= repetitions; ++rep)
		printf(
			"%s\t%lu\t%lf\t%lf\n",
			get_focus_buffer(),
			rep,
			unfairness_from_daemon(),
			unfairness_from_daemon()
		);
}

RerankMethod rerank_method_from_argv(int &argc, char ***argv) {
	if (argc == 0) return RerankMethod::INDIVIDUAL_AMORTIZED_FAIRNESS;
	if (strcmp(**argv, "iaf") == 0) {
		(*argv)++;
		argc--;
		return RerankMethod::INDIVIDUAL_AMORTIZED_FAIRNESS;
	}
	if (strcmp(**argv, "fairco") == 0) {
		(*argv)++;
		argc--;
		return RerankMethod::GROUP_FAIRCO;
	}
	return RerankMethod::INDIVIDUAL_AMORTIZED_FAIRNESS;
}

int main(int argc, char *argv[]) {
	argv++;
	argc--;
	Subcommand subcommand;
	bool input_pipe_open = !isatty(fileno(stdin));
	if (argc == 0) subcommand = input_pipe_open ? RERANK : HELP;
	else if (strcmp(*argv, "help") == 0) subcommand = HELP;
	else if (strcmp(*argv, "rerank") == 0) subcommand = RERANK;
	else if (strcmp(*argv, "candidates") == 0) subcommand = SHOW_CANDIDATES;
	else if (strcmp(*argv, "daemon") == 0) subcommand = DAEMON;
	else if (strcmp(*argv, "experiment") == 0) subcommand = EXPERIMENT;
	else if (strcmp(*argv, "convert-click-log") == 0)
		subcommand = CONVERT_CLICK_LOG;
	else if (strcmp(*argv, "convert-dense-click-log") == 0)
		subcommand = CONVERT_DENSE_CLICK_LOG;
	else if (strcmp(*argv, "read-sparse-click-log") == 0)
		subcommand = READ_SPARSE_CLICK_LOG;
	else if (strcmp(*argv, "read-dense-click-log") == 0)
		subcommand = READ_DENSE_CLICK_LOG;
	else if (strcmp(*argv, "click-log-probable-rank-docs") == 0)
		subcommand = CLICK_LOG_PROBABLE_RANK_DOCS;
	else if (strcmp(*argv, "click-log-predict-rankings") == 0)
		subcommand = CLICK_LOG_PREDICT_RANKINGS;
	else if (strcmp(*argv, "pscm") == 0)
		subcommand = PARTIALLY_SEQUENTIAL_CLICK_MODEL;
	else if (strcmp(*argv, "read-config") == 0) subcommand = READ_CONFIG;
	else if (strcmp(*argv, "read-rel-file") == 0)
		subcommand = READ_RELEVANCY_FILE;
	else {
		argv--;
		argc++;
		subcommand = RERANK;
	}
	argv++;
	argc--;
	bool extra_args = argc > 0;
	f64 quality_threshold;
	RerankMethod method;
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
		case CONVERT_DENSE_CLICK_LOG: {
			if (!extra_args || input_pipe_open)
				print_subcommand_help_then_halt(CONVERT_DENSE_CLICK_LOG, 1);
			char *click_log_path = *argv;
			argv++;
			argc--;
			char *rankings_path = *argv;
			argv++;
			argc--;
			convert_dense_click_log(click_log_path, rankings_path);
			break;
		} case READ_CONFIG:
			if (extra_args || input_pipe_open)
				print_subcommand_help_then_halt(READ_CONFIG, 1);
			read_config();
			break;
		case CONVERT_CLICK_LOG:
			if (extra_args || !input_pipe_open)
				print_subcommand_help_then_halt(CONVERT_CLICK_LOG, 1);
			convert_click_log();
			break;
		case READ_SPARSE_CLICK_LOG:
			if (extra_args || !input_pipe_open)
				print_subcommand_help_then_halt(READ_SPARSE_CLICK_LOG, 1);
			read_sparse_click_log();
			break;
		case READ_DENSE_CLICK_LOG:
			if (extra_args || !input_pipe_open)
				print_subcommand_help_then_halt(READ_DENSE_CLICK_LOG, 1);
			read_dense_click_log();
			break;
		case CLICK_LOG_PROBABLE_RANK_DOCS:
			if (extra_args || !input_pipe_open)
				print_subcommand_help_then_halt(
					CLICK_LOG_PROBABLE_RANK_DOCS, 1);
			click_log_probable_rank_docs();
			break;
		case CLICK_LOG_PREDICT_RANKINGS: {
			if (extra_args || !input_pipe_open)
				print_subcommand_help_then_halt(CLICK_LOG_PREDICT_RANKINGS, 1);
			click_log_predict_rankings();
			break;
		} case RERANK:
			if (!input_pipe_open)
				print_subcommand_help_then_halt(RERANK, 1);
			method = rerank_method_from_argv(argc, &argv);
			extra_args = argc > 0;
			if (method == RerankMethod::INDIVIDUAL_AMORTIZED_FAIRNESS) {
				if (extra_args) quality_threshold = atof(*argv);
				else quality_threshold = 0.99;
				iaf_rerank(quality_threshold);
			} else if (method == RerankMethod::GROUP_FAIRCO) fairco_rerank();
			break;
		case SHOW_CANDIDATES:
			if (!input_pipe_open)
				print_subcommand_help_then_halt(RERANK, 1);
			if (extra_args) quality_threshold = atof(*argv);
			else quality_threshold = 0.99;
			show_candidates(quality_threshold);
			break;
		case DAEMON:
			if (extra_args || input_pipe_open)
				print_subcommand_help_then_halt(DAEMON, 1);
			start_daemon();
			break;
		case EXPERIMENT:
			if (!extra_args || !input_pipe_open)
				print_subcommand_help_then_halt(EXPERIMENT, 1);
			method = rerank_method_from_argv(argc, &argv);
			extra_args = argc > 0;
			ui64 repetitions;
			if (method == RerankMethod::INDIVIDUAL_AMORTIZED_FAIRNESS) {
				if (extra_args) {
					printf("Extra args\n");
					printf("%s\n", *argv);
					quality_threshold = atof(*argv);
					argv++;
					argc--;
					extra_args = argc > 0;
					if (extra_args) {
						repetitions = atoll(*argv);
						argv++;
						argc--;
					}
					else repetitions = 3000;
				} else {
					quality_threshold = 0.99;
					repetitions = 3000;
				}
				printf("Quality threshold: %lf\n", quality_threshold);
				iaf_experiment(quality_threshold, repetitions);
			} else if (method == RerankMethod::GROUP_FAIRCO) {
				if (extra_args) {
					repetitions = atoll(*argv);
					argv++;
					argc--;
				}
				else repetitions = 3000;
				fairco_experiment(repetitions);
			}
			break;
		case PARTIALLY_SEQUENTIAL_CLICK_MODEL:
			if (extra_args || !input_pipe_open)
				print_subcommand_help_then_halt(
					PARTIALLY_SEQUENTIAL_CLICK_MODEL, 1);
			partially_sequential_click_model();
			break;
		case READ_RELEVANCY_FILE:
			if (extra_args || !input_pipe_open)
				print_subcommand_help_then_halt(READ_RELEVANCY_FILE, 1);
			read_relevancy_file();
			break;
		case BAD_SUBCOMMAND:
			print_general_help_then_halt();
	}
}
