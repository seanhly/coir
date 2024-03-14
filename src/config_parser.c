#include "fp_utils.c"

typedef enum {
	START,
	COMMENT,
	SECTION_TITLE_START,
	SECTION_TITLE_MIDDLE,
	SECTION_TITLE_PRE_END,
	SECTION_TITLE_END,
	KEY_VAL_START,
	KEY_MIDDLE,
	KEY_END,
	VAL_START,
	VAL_MIDDLE,
	VAL_END,
	END,
} ParseState;

typedef enum {
	DEFAULT,
	CLICK_MODELS,
	LOGGING,
} ConfigSection;

typedef enum {
	CONFIDENCE,
	LEVEL,
} ConfigKey;

typedef struct {
	char *start;
	char *ptr;
} Buffer;

typedef struct {
	ui64 ci;
	char c;
	ParseState s;
	Buffer buffer;
	ConfigSection section;
	ConfigKey key;
	Configuration config;
} ConfigParseState;

void unexpected_and_exit(ConfigParseState *state) {
	fprintf(stderr, "unexpected character at %lu: <%c>\n",
		state->ci, state->c);
	exit(1);
}

void start_state_trans(ConfigParseState *state) {
	switch (state->c) {
		case '\n':
			break;
		case ' ':
			break;
		case '\t':
			break;
		case ';':
			state->s = COMMENT;
			break;
		case '[':
			state->s = SECTION_TITLE_START;
			break;
		default: unexpected_and_exit(state);
	}
}

void section_title_start_trans(ConfigParseState *state) {
	switch (state->c) {
		case '\n':
			break;
		case ' ':
			break;
		case '\t':
			break;
		case ']':
			fprintf(stderr, "no section title at %lu: <%u>\n",
				state->ci, state->c);
			exit(1);
		default:
			*(state->buffer.ptr++) = state->c;
			state->s = SECTION_TITLE_MIDDLE;
	}
}

void section_from_title(ConfigParseState *state) {
	*(state->buffer.ptr) = '\0';
	if (strcmp(state->buffer.start, "click-models") == 0)
		state->section = CLICK_MODELS;
	else if (strcmp(state->buffer.start, "logging") == 0)
		state->section = LOGGING;
	else {
		fprintf(stderr, "unknown section: <%s>\n", state->buffer.start);
		exit(1);
	}
	state->buffer.ptr = state->buffer.start;
}

void key_from_title(ConfigParseState *state) {
	*(state->buffer.ptr) = '\0';
	if (strcmp(state->buffer.start, "confidence") == 0)
		state->key = CONFIDENCE;
	else if (strcmp(state->buffer.start, "level") == 0)
		state->key = LEVEL;
	else {
		fprintf(stderr, "unknown key: <%s>\n", state->buffer.start);
		exit(1);
	}
	state->buffer.ptr = state->buffer.start;
}

void section_title_middle_trans(ConfigParseState *state) {
	switch (state->c) {
		case '\n':
		case ' ':
		case '\t':
			*(state->buffer.ptr) = '\0';
			state->s = SECTION_TITLE_PRE_END;
			break;
		case ']':
			section_from_title(state);
			state->s = SECTION_TITLE_END;
			break;
		default:
			*(state->buffer.ptr) = state->c;
			state->buffer.ptr++;
	}
}

void section_title_pre_end_trans(ConfigParseState *state) {
	switch (state->c) {
		case '\n':
		case ' ':
		case '\t':
			break;
		case ']':
			state->s = SECTION_TITLE_END;
			section_from_title(state);
			break;
		default: unexpected_and_exit(state);
	}
}

void section_title_end_trans(ConfigParseState *state) {
	switch (state->c) {
		case '\n':
			state->s = KEY_VAL_START;
			break;
		case ' ':
		case '\t':
			break;
		case ';':
			state->s = COMMENT;
			break;
		default: unexpected_and_exit(state);
	}
}

void key_val_start_trans(ConfigParseState *state) {
	switch (state->c) {
		case '\n':
		case ' ':
		case '\t':
			break;
		case ';':
			state->s = COMMENT;
			break;
		default:
			*(state->buffer.ptr++) = state->c;
			state->s = KEY_MIDDLE;
	}
}

void key_start_trans(ConfigParseState *state) {
	switch (state->c) {
		case '\n':
		case ' ':
		case '\t':
			break;
		case ';':
			state->s = COMMENT;
			break;
		default:
			*(state->buffer.ptr++) = state->c;
			state->s = KEY_MIDDLE;
	}
}

void key_middle_trans(ConfigParseState *state) {
	switch (state->c) {
		case ';':
		case '\n': unexpected_and_exit(state);
		case ' ':
		case '\t':
			state->s = KEY_END;
			key_from_title(state);
			break;
		default:
			*(state->buffer.ptr) = state->c;
			state->buffer.ptr++;
			state->s = KEY_MIDDLE;
	}
}

void key_end_trans(ConfigParseState *state) {
	switch (state->c) {
		case ';':
		case '\n': unexpected_and_exit(state);
		case ' ':
		case '\t': break;
		case '=':
		case ':':
			state->s = VAL_START;
			break;
		case '"':
			state->s = VAL_MIDDLE;
			break;
		default: unexpected_and_exit(state);
	}
}

void val_start_trans(ConfigParseState *state) {
	switch (state->c) {
		case ';':
		case '\n':
			state->s = VAL_END;
		case ' ':
		case '\t': break;
		default:
			*(state->buffer.ptr) = state->c;
			state->buffer.ptr++;
			state->s = VAL_MIDDLE;
	}
}

void set_config_val(ConfigParseState *state) {
	switch (state->section) {
		case CLICK_MODELS:
			switch (state->key) {
				case CONFIDENCE:
					*(state->buffer.ptr) = '\0';
					f64 p = atof(state->buffer.start);
					if (p < 1)
						state->config.CM_confidence = (percentage) (p * 100);
					else
						state->config.CM_confidence = (percentage) p;
					state->buffer.ptr = state->buffer.start;
			}
			break;
		case LOGGING:
			switch (state->key) {
				case LEVEL:
					*(state->buffer.ptr) = '\0';
					state->config.logging_level = atoi(state->buffer.start);
					state->buffer.ptr = state->buffer.start;
			}
			break;
		default:
			*(state->buffer.ptr) = '\0';
			fprintf(stderr, "confused about value %s at %lu\n", state->buffer.start, state->ci);
			exit(1);
	}
}

void val_middle_trans(ConfigParseState *state) {
	switch (state->c) {
		case ';':
		case EOF:
		case '\n':
			set_config_val(state);
			state->s = VAL_END;
		case ' ':
		case '\t': break;
		default:
			*(state->buffer.ptr) = state->c;
			state->buffer.ptr++;
			state->s = VAL_MIDDLE;
	}
}

void val_end_trans(ConfigParseState *state) {
	switch (state->c) {
		case ';':
			state->s = COMMENT;
		case '\n':
		case ' ':
		case '\t':
			break;
		case '[':
			state->s = SECTION_TITLE_START;
			break;
		case EOF:
			state->s = END;
			break;
	}
}

Configuration parse_config_file(FILE *fp) {
	ConfigParseState state;
	state.s = START;
	state.ci = 0;
	state.section = DEFAULT;
	state.buffer.start = (char*) malloc(1024);
	state.buffer.ptr = state.buffer.start;
	state.config = DEFAULT_CONFIG;
	while (state.s != END) {
		state.c = fgetc(fp);
		switch (state.s) {
			case START:
				start_state_trans(&state);
				break;
			case SECTION_TITLE_START:
				section_title_start_trans(&state);
				break;
			case SECTION_TITLE_MIDDLE:
				section_title_middle_trans(&state);
				break;
			case SECTION_TITLE_PRE_END:
				section_title_pre_end_trans(&state);
				break;
			case SECTION_TITLE_END:
				section_title_end_trans(&state);
				break;
			case KEY_VAL_START:
				key_val_start_trans(&state);
				break;
			case KEY_MIDDLE:
				key_middle_trans(&state);
				break;
			case KEY_END:
				key_end_trans(&state);
				break;
			case VAL_START:
				val_start_trans(&state);
				break;
			case VAL_MIDDLE:
				val_middle_trans(&state);
				break;
			case VAL_END:
				val_end_trans(&state);
				break;
		}
		state.ci++;
	}
	return state.config;
}
