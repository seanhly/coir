void geometric_attention(ui8 *attention, double click_probability) {
	double *attention_double = (double*) attention;
	attention_double[0] = 1 - click_probability;
	double multiplicand = 1 - click_probability;
	for (int i = 1; i < PAGE_LENGTH; ++i) attention_double[i] =
		pow(multiplicand, i + 1) + attention_double[i - 1];
	for (int i = 0; i < PAGE_LENGTH; ++i) attention[i] = (ui8) floor(
		attention_double[i] / attention_double[PAGE_LENGTH - 1] * 255
	);
	for (int i = PAGE_LENGTH - 1; i >= 1; --i)
		attention[i] = attention[i] - attention[i - 1];
	std::sort(
		attention, attention + PAGE_LENGTH, std::greater<ui8>()
	);
}

void click_model_attention(ui8 *attention, Candidate *candidates) {
	double *attention_double = (double*) attention;
	attention_double[0] = 1;
	for (int i = 1; i < PAGE_LENGTH; ++i)
		attention_double[i] = attention_double[i - 1] * (1 - candidates[i - 1].relevance);
	for (int i = 0; i < PAGE_LENGTH; ++i)
		attention_double[i] *= attention_double[i] * candidates[i].relevance;
	for (int i = 1; i < PAGE_LENGTH; ++i)
		attention_double[i] += attention_double[i - 1];
	for (int i = 0; i < PAGE_LENGTH; ++i) attention[i] = (ui8) floor(
		attention_double[i] / attention_double[PAGE_LENGTH - 1] * 255
	);
	for (int i = PAGE_LENGTH - 1; i >= 1; --i)
		attention[i] = attention[i] - attention[i - 1];
	std::sort(
		attention, attention + PAGE_LENGTH, std::greater<ui8>()
	);
}

void exposure_model_attention(ui8 *attention, Candidate *candidates) {
	double *attention_double = (double*) attention;
	attention_double[0] = 1;
	for (int i = 1; i < PAGE_LENGTH; ++i)
		attention_double[i] = attention_double[i - 1] * (1 - candidates[i - 1].relevance);
	for (int i = 1; i < PAGE_LENGTH; ++i)
		attention_double[i] += attention_double[i - 1];
	for (int i = 0; i < PAGE_LENGTH; ++i) attention[i] = (ui8) floor(
		attention_double[i] / attention_double[PAGE_LENGTH - 1] * 255
	);
	for (int i = PAGE_LENGTH - 1; i >= 1; --i)
		attention[i] = attention[i] - attention[i - 1];
	std::sort(
		attention, attention + PAGE_LENGTH, std::greater<ui8>()
	);
}
