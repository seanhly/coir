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

typedef struct {
	ui32 d;
	expected relevance;
} DocRel;

int compare_doc_rel(const void *void_a, const void *void_b) {
	DocRel *ptr_a = (DocRel*) void_a;
	DocRel *ptr_b = (DocRel*) void_b;
	if (ptr_b->relevance < ptr_a->relevance) return -1;
	if (ptr_a->relevance < ptr_b->relevance) return +1;
	if (ptr_a->d < ptr_b->d) return -1;
	if (ptr_b->d < ptr_a->d) return +1;
	return 0;
}

void confident_min_relevance(probability confidence) {
	if (confidence < 0) confidence = config().CM_confidence;
	const f64 z = z_score(confidence);
	const f64 no_infnty = 1e-18;
	const f64 omega = 1 / (z * z + no_infnty);
	ui64 max_run;
	safe_read(&max_run, sizeof(ui64), stdin);
	DocRel *doc_rel_buffer = (DocRel*) malloc(max_run * sizeof(DocRel));
	ui32 query;
	while (fread(&query, sizeof(ui32), 1, stdin) == 1) {
		ui64 doc_rel_c;
		safe_read(&doc_rel_c, sizeof(ui64), stdin);
		ui64 doc_rel_buffer_c = 0;
		while (doc_rel_c-- > 0) {
			ui32 d;
			probability relevance;
			ui32 clicks;
			safe_read(&d, sizeof(ui32), stdin);
			safe_read(&clicks, sizeof(ui32), stdin);
			safe_read(&relevance, sizeof(probability), stdin);
			expected view_count =
				clicks / relevance;
			expected standard_error = sqrt(
				(
					(
						(clicks + omega) / view_count
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
			probability minimal_relevance =
				MAX(relevance - margin_of_error, 0);
			if (minimal_relevance > 0.000001)
				doc_rel_buffer[doc_rel_buffer_c++] = {d, minimal_relevance};
		}
		qsort(
			doc_rel_buffer, doc_rel_buffer_c, sizeof(DocRel), compare_doc_rel
		);
		if (doc_rel_buffer_c > 0) {
			fprintf(stderr, "Query: %u\n", query);
			fprintf(stderr, "\tDoc rels: %lu\n", doc_rel_buffer_c);
			for (ui64 i = 0; i < doc_rel_buffer_c; ++i) {
					fprintf(
						stderr,
						"\t\td:%u min-rel:%f\n",
						doc_rel_buffer[i].d,
						doc_rel_buffer[i].relevance
					);
			}
		}
	}
}
