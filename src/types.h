typedef unsigned long ui64;
typedef double f64;
typedef unsigned char ui8;

typedef enum {
	HELP,
	RERANK,
	DAEMON,
	SHOW_CANDIDATES,
	BAD_SUBCOMMAND
} Subcommand;

typedef enum {
	UPDATE_RELEVANCE,
	GEN_RANKING,
	GET_CANDIDATES
} Job;

typedef enum {
	DELETE,
	INSERT
} UpdateType;

void handleErrors(void) {
	ERR_print_errors_fp(stderr);
	abort();
}

typedef struct {
	ui64 doc_id;
	f64 relevance;
	ui64 exposure;
	bool masked;
} Candidate;

typedef struct {
	ui64 candidates_c;
	Candidate **candidates_ptr;
} RankContext;

typedef struct {
	RankContext *rank_contexts;
	ui64 rank_contexts_c;
	RankContext **rank_contexts_per_rank;
	Candidate *candidates_ptr;
	ui64 candidates_c;
	f64 min_dcg;
} Context;

typedef struct {
	char *buffer;
	ui64 length;
} PathBuffer;

typedef struct {
	Candidate candidate;
	UpdateType type;
} UpdateAction;
