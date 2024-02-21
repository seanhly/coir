double discounted_gain(
	double relevance,
	unsigned long rank
) {
	return (pow(2, relevance) - 1) / log2(rank + 1);
}

/**
 * candidates is ordered by relevance in descending order.
 */
double min_rel_priori_dcg(
	Candidate *candidates,
	unsigned long i,
	double quality_threshold
) {
	if (quality_threshold == 1) return candidates[i - 1].relevance;
	double before_weight = 0;
	for (unsigned long j = 1; j <= i - 1; ++j)
		before_weight += discounted_gain(candidates[j - 1].relevance, j);
	double idcg = before_weight;
	for (unsigned long j = i; j <= PAGE_LENGTH; ++j)
		idcg += discounted_gain(candidates[j - 1].relevance, j);
	double after_weight = 0;
	for (unsigned long j = i + 1; j <= PAGE_LENGTH; ++j)
		after_weight += discounted_gain(candidates[j - 2].relevance, j);
	double weve_got_this = before_weight + after_weight;
	double min_dcg = quality_threshold * idcg;
	if (weve_got_this > min_dcg) return 0;
	double min_rel_priori = log2(1 + log2(i + 1) * (min_dcg - weve_got_this));
	if (min_rel_priori < candidates[PAGE_LENGTH - 1].relevance) return min_rel_priori;
	long candidate_rank = lowest_above_or_equals(
		candidates, PAGE_LENGTH, min_rel_priori) + 1;
	double running_dcg = before_weight;
	double i_weight = discounted_gain(candidates[candidate_rank - 1].relevance, i);
	running_dcg += i_weight;
	for (int j = i + 1; j <= candidate_rank; ++j)
		running_dcg += discounted_gain(candidates[j - 2].relevance, j);
	for (int j = candidate_rank + 1; j <= PAGE_LENGTH; ++j)
		running_dcg += discounted_gain(candidates[j - 1].relevance, j);
	while (running_dcg < min_dcg && candidate_rank != i) {
		running_dcg -= i_weight;
		running_dcg -= discounted_gain(candidates[candidate_rank - 2].relevance, candidate_rank);
		running_dcg += discounted_gain(candidates[candidate_rank - 1].relevance, candidate_rank);
		i_weight = discounted_gain(candidates[candidate_rank - 2].relevance, i);
		running_dcg += i_weight;
		candidate_rank -= 1;
	}
	return candidates[candidate_rank - 1].relevance;
}

// For testing purposes (above implementation used instead)
double min_rel_priori_dcg_deoptimised(
	Candidate *candidates,
	unsigned long i,
	double quality_threshold
) {
	if (quality_threshold == 1) return candidates[i - 1].relevance;
	double before_weight = 0;
	for (unsigned long j = 1; j <= i - 1; ++j)
		before_weight += discounted_gain(candidates[j - 1].relevance, j);
	double idcg = before_weight;
	for (unsigned long j = i; j <= PAGE_LENGTH; ++j)
		idcg += discounted_gain(candidates[j - 1].relevance, j);
	double after_weight = 0;
	for (unsigned long j = i + 1; j <= PAGE_LENGTH; ++j)
		after_weight += discounted_gain(candidates[j - 2].relevance, j);
	double weve_got_this = before_weight + after_weight;
	double min_dcg = quality_threshold * idcg;
	if (weve_got_this > min_dcg) return 0;
	double min_rel_priori = log2(1 + log2(i + 1) * (min_dcg - weve_got_this));
	if (min_rel_priori < candidates[PAGE_LENGTH - 1].relevance) return min_rel_priori;
	long candidate_rank = lowest_above_or_equals(
		candidates, PAGE_LENGTH, min_rel_priori) + 1;
	do {
		double running_dcg = before_weight;
		double weight = discounted_gain(candidates[candidate_rank - 1].relevance, i);
		running_dcg += weight;
		for (int j = i + 1; j <= candidate_rank; ++j)
			running_dcg += discounted_gain(candidates[j - 2].relevance, j);
		for (int j = candidate_rank + 1; j <= PAGE_LENGTH; ++j)
			running_dcg += discounted_gain(candidates[j - 1].relevance, j);
		if (running_dcg >= min_dcg) break;
		candidate_rank -= 1;
	} while (candidate_rank > i);
	return candidates[candidate_rank - 1].relevance;
}
