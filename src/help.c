void print_general_help_then_halt() {
	printf("%s\n", HELP_MESSAGE);
	exit(0);
}

void print_data_help_then_halt() {
	exit(0);
}

void print_subcommand_help_then_halt(Subcommand subcommand, int return_code) {
	switch (subcommand) {
		case RERANK: printf("%s\n", RERANK_HELP_MESSAGE); break;
		case DAEMON: printf(DAEMON_HELP_MESSAGE); break;
		case HELP: printf("%s\n", HELP_HELP_MESSAGE); break;
		case BAD_SUBCOMMAND: printf("%s\n", HELP_HELP_MESSAGE); exit(1);
		default: print_general_help_then_halt();
	}
	exit(return_code);
}
