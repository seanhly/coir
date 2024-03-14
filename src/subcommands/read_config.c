#include "../config.c"

void read_config() {
	Configuration c = config();
	printf("Click model confidence: %u\n", c.CM_confidence);
	printf("Logging level: %u\n", c.logging_level);
}
