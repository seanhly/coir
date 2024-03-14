#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>


void read_sparse_click_log() {
	ui32 qid;
	ui32 session_c;
	ui32 sid;
	ui32 duplicates;
	ui16 click_c;
	ui16 pos;
	ui32 url;
	while (fread(&qid, sizeof(ui32), 1, stdin) == 1) {
		safe_read(&session_c, sizeof(ui32), stdin);
		printf("qid: %u\n", qid);
		printf("session_c: %u\n", session_c);
		for (ui32 i = 0; i < session_c; ++i) {
			safe_read(&sid, sizeof(ui32), stdin);
			safe_read(&duplicates, sizeof(ui32), stdin);
			printf("\tsid: %u\n", sid);
			printf("\tduplicates: %u\n", duplicates);
			safe_read(&click_c, sizeof(ui16), stdin);
			printf("\tclick_c: %u\n", click_c);
			for (ui32 j = 0; j < click_c; ++j) {
				safe_read(&pos, sizeof(ui16), stdin);
				safe_read(&url, sizeof(ui32), stdin);
				printf("\t\tpos: %u\n", pos);
				printf("\t\turl: %u\n", url);
			}
		}
	}
}
