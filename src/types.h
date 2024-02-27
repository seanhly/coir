typedef unsigned long ui64;
typedef double f64;
typedef unsigned char ui8;
typedef unsigned short ui16;
typedef ui64 docid;
typedef ui64 qid;
typedef ui64 job_id;
typedef ui8 gid;

typedef enum {
	HELP,
	RERANK,
	DAEMON,
	SHOW_CANDIDATES,
	EXPERIMENT,
	BAD_SUBCOMMAND,
	FAIRCO_RERANK
} Subcommand;

typedef enum {
	UPDATE_RELEVANCE,
	GEN_RANKING,
	GET_CANDIDATES,
	LAUNCH_IAF_EXPERIMENT,
	LAUNCH_FAIRCO_EXPERIMENT,
	GEN_FAIRCO_RE_RANKING,
    STOP_DAEMON
} Job;

typedef enum {
	INDIVIDUAL_AMORTIZED_FAIRNESS,
	GROUP_FAIRCO
} RerankMethod;

typedef struct {
	docid doc;
	f64 relevance;
	ui64 exposure;
	bool masked;
} Candidate;

typedef struct {
	ui64 candidates_c;
	Candidate **candidates_ptr;
} CandidateList;

typedef struct {
	ui64 rank;
	Candidate *candidate;
} RankedItem;

typedef struct {
	CandidateList candidates;
	ui64 exposure;
	f64 bulk_relevance;
	ui64 offset;
	f64 error;
} GroupContext;

typedef struct {
	CandidateList *rank_contexts;
	ui64 rank_contexts_c;
	CandidateList **rank_contexts_per_rank;
	Candidate *candidates_ptr;
	ui64 candidates_c;
	f64 idcg;
	f64 quality_threshold;
	GroupContext *group_contexts;
	unsigned short groups_c;
	GroupContext *majority_group;
} Context;

typedef struct {
	char *buffer;
	ui64 length;
} PathBuffer;

typedef enum {
	STANDALONE,
	PART_OF_EXPERIMENT
} RankingMode;
