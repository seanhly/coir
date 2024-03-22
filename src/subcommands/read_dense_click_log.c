#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>


void read_dense_click_log() {
	ui8 file_opts;
	safe_read(&file_opts, sizeof(ui8), stdin);
	bool concrete = (file_opts & CONCRETE_RANK_DATA) != 0;
	ui32 qid;
	ui32 session_c;
	ui32 sid;
	ui32 duplicates;
	ui16 click_c;
	ui16 rank;
	ui32 doc;
	while (fread(&session_c, sizeof(ui32), 1, stdin) == 1) {
		printf("qid: %u\n", qid);
		printf("session_c: %u\n", session_c);
		for (ui32 i = 0; i < session_c; ++i) {
			safe_read(&duplicates, sizeof(ui32), stdin);
			printf("\tduplicates: %u\n", duplicates);
			safe_read(&click_c, sizeof(ui16), stdin);
			printf("\tclick_c: %u\n", click_c);
			ui16 max_rank = 0;
			ui16 prev_rank = 0;
			for (ui32 j = 0; j < click_c; ++j) {
				if (j == 0) {
					safe_read(&rank, sizeof(ui16), stdin);
				} else {
					i8 gap;
					safe_read(&gap, sizeof(i8), stdin);
					rank = prev_rank + gap;
				}
				if (rank > max_rank) max_rank = rank;
				printf("\t\t>>> CLICKED <<< %u\n", rank);
				prev_rank = rank;
			}
			if (concrete) {
				printf("\t\t");
				for (ui16 j = 0; j < max_rank; ++j) {
					safe_read(&doc, sizeof(ui32), stdin);
					printf("doc:%u ", doc);
				}
				printf("\n");
			} else {
				ui32 total_mass;
				for (ui16 j = 0; j < max_rank; ++j) {
					safe_read(&total_mass, sizeof(ui32), stdin);
					printf("\t\tTOTAL: %u\n", total_mass);
					ui32 mass;
					for (ui32 k = 0; k < total_mass; k += mass) {
						safe_read(&doc, sizeof(ui32), stdin);
						safe_read(&mass, sizeof(ui32), stdin);
						printf("\t\t\tdoc:%u(%u)\n", doc, mass);
					}
				}
			}
		}
		++qid;
	}
}
