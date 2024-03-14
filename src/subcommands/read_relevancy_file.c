#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>


void read_relevancy_file() {
	ui32 qid;
	ui64 result_c;
	ui32 doc;
	f64 relevance;
	printf("qid,doc,run,relevance\n");
	while (fread(&qid, sizeof(ui32), 1, stdin) == 1) {
		safe_read(&result_c, sizeof(ui64), stdin);
		for (ui64 i = 0; i < result_c; ++i) {
			safe_read(&doc, sizeof(ui32), stdin);
			safe_read(&relevance, sizeof(f64), stdin);
			printf("%u,%u,%lu,%f\n", qid, doc, i + 1, relevance);
		}
	}
}
