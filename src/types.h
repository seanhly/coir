typedef unsigned long ui64;
typedef short i16;
typedef int i32;
typedef bool b;
typedef double f64;
typedef unsigned char ui8;
typedef char i8;
typedef unsigned short ui16;
typedef unsigned int ui32;
typedef ui64 docid;
typedef ui64 qid;
typedef ui64 job_id;
typedef ui8 gid;
typedef f64 probability;
typedef f64 expected;
typedef ui8 percentage;

typedef enum {
	HELP,
	RERANK,
	DAEMON,
	SHOW_CANDIDATES,
	EXPERIMENT,
	BAD_SUBCOMMAND,
	FAIRCO_RERANK,
	CONVERT_CLICK_LOG,
	READ_SPARSE_CLICK_LOG,
	CLICK_LOG_PROBABLE_RANK_DOCS,
	CLICK_LOG_PREDICT_RANKINGS,
	PARTIALLY_SEQUENTIAL_CLICK_MODEL,
	READ_CONFIG,
	READ_RELEVANCY_FILE,
	CONVERT_DENSE_CLICK_LOG,
	READ_DENSE_CLICK_LOG,
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
	ui16 groups_c;
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

typedef struct {
	ui16 pos;
	ui32 doc;
} Click;

typedef struct {
	ui16 click_c;
	Click *clicks;
	ui32 sid;
	ui32 duplicates;
} Session;

typedef struct {
	ui32 query;
	ui32 session_c;
	Session *sessions;
} Query;
