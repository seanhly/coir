#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>


void read_relevancy_file() {
	ui32 query;
	while (fread(&query, sizeof(ui32), 1, stdin) == 1) {
		while (1) {
			ui8 relevance8;
			safe_read(&relevance8, sizeof(ui8), stdin);
			probability relevance = relevance8 / 255.0;
			if (relevance8 == 0) break;
			ui32 d;
			safe_read(&d, sizeof(ui32), stdin);
			printf("q: %u, d: %u, relevance: %f\n", query, d, relevance);
		}
	}
}
