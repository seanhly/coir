void a() {
	printf("Checkpoint: A\n");
}

void b() {
	printf("Checkpoint: B\n");
}

void c() {
	printf("Checkpoint: C\n");
}

void d() {
	printf("Checkpoint: D\n");
}

void e() {
	printf("Checkpoint: E\n");
}

// Alias for printf + fflush (with args)
void p(const char *fmt, ...) {
	va_list args;
	va_start(args, fmt);
	vprintf(fmt, args);
	va_end(args);
	fflush(stdout);
}
