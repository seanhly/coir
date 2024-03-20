#define HELP_MESSAGE "Usage: librecoir [SUBCOMMAND] [ARGUMENTS]\n"

const char *HELP_HELP_MESSAGE =
"Usage: librecoir help\n"
"       librecoir help [SUBCOMMAND] [ARGUMENTS]\n"
"\n"
"Prints help information (generally or for a specific subcommand)."
;
const char *RERANK_HELP_MESSAGE =
"Usage: librecoir iaf_rerank [QID]\n"
"       librecoir [QID]\n"
"       librecoir iaf_rerank [QID] < [QREL_FILE (1)]\n"
"       librecoir iaf_rerank < [QREL_FILE (2)]\n"
"\n"
"Produces a re-ranking for the documents in the QREL_FILE, given a query ID."
"\n"
"QREL files come in two formats:\n"
"\n"
"QREL_FILE (1):\n"
"	<DOC_ID> <TAB> <RELEVANCE> <NEWLINE>\n"
"	<DOC_ID> <TAB> <RELEVANCE> <NEWLINE>\n"
"	...\n"
"\n"
"QREL_FILE (2):\n"
"	<QID> <NEWLINE>\n"
"	<DOC_ID> <TAB> <RELEVANCE> <NEWLINE>\n"
"	<DOC_ID> <TAB> <RELEVANCE> <NEWLINE>\n"
"	...\n"
"\n"
"The first format is used when the query ID is provided as a command-line argument.\n"
"The second format is used when the query ID is provided via standard input."
;
#define DAEMON_HELP_MESSAGE "Usage: librecoir daemon\n"
