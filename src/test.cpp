#include <math.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include "types.h"
#include "attention.cpp"
#include "config.c"
#include "utils.c"
#include "metrics.cpp"
#include "constants.h"


void pass(const char *message, ...) {
	printf("[%sPASSED%s] ", GREEN, RESET);
	va_list args;
	va_start(args, message);
	vprintf(message, args);
	va_end(args);
	printf("\n");
}

void failure(const char *message, ...) {
	printf("[%sFAILED%s] ", RED, RESET);
	va_list args;
	va_start(args, message);
	vprintf(message, args);
	va_end(args);
	printf("\n");
	exit(1);
}

void exposure_model_attention_test(unsigned char *attention, double *numbers) {
	Candidate *candidates = (Candidate*) malloc(PAGE_LENGTH * sizeof(Candidate));
	for (int i = 0; i < PAGE_LENGTH; ++i) candidates[i].relevance = numbers[i];
	exposure_model_attention(attention, candidates);
}

void click_model_attention_test(unsigned char *attention, double *numbers) {
	Candidate *candidates = (Candidate*) malloc(PAGE_LENGTH * sizeof(Candidate));
	for (int i = 0; i < PAGE_LENGTH; ++i) candidates[i].relevance = numbers[i];
	click_model_attention(attention, candidates);
}

void test_attention_models() {
	unsigned char *attention =
		(unsigned char*) malloc(PAGE_LENGTH * sizeof(double));
	geometric_attention(attention, 0.7);
	for (int i = 0; i < PAGE_LENGTH; ++i)
		printf("%d ", attention[i]);
	printf("\n");
	double *click_model = (double*) malloc(PAGE_LENGTH * sizeof(double));
	click_model[0] = 0.85;
	click_model[1] = 0.8;
	click_model[2] = 0.7;
	click_model[3] = 0.6;
	click_model[4] = 0.5;
	click_model[5] = 0.4;
	click_model[6] = 0.3;
	click_model[7] = 0.2;
	click_model[8] = 0.1;
	click_model[9] = 0.05;
	exposure_model_attention_test(attention, click_model);
	for (int i = 0; i < PAGE_LENGTH; ++i)
		printf("%d ", attention[i]);
	printf("\n");
	click_model_attention_test(attention, click_model);
	for (int i = 0; i < PAGE_LENGTH; ++i)
		printf("%d ", attention[i]);
	printf("\n");
}

long lowest_above_or_equals_test(double *arr, long len, double val) {
	Candidate *candidates = (Candidate*) malloc(len * sizeof(Candidate));
	for (long i = 0; i < len; ++i) {
		candidates[i].relevance = arr[i];
		candidates[i].doc_id = i;
	}
	return lowest_above_or_equals(candidates, len, val);
}

long min_rel_priori_dcg_deoptimised_test(double *arr, long a, long b) {
	Candidate *candidates = (Candidate*) malloc(a * sizeof(Candidate));
	for (long i = 0; i < a; ++i) {
		candidates[i].relevance = arr[i];
		candidates[i].doc_id = i;
	}
	return min_rel_priori_dcg_deoptimised(candidates, a, b);
}

long min_rel_priori_dcg_test(double *arr, long a, long b) {
	Candidate *candidates = (Candidate*) malloc(a * sizeof(Candidate));
	for (long i = 0; i < a; ++i) {
		candidates[i].relevance = arr[i];
		candidates[i].doc_id = i;
	}
	return min_rel_priori_dcg(candidates, a, b);
}

void test_lowest_above_or_equals() {
	double arr2[] = {100.2, 90.7, 70.3, 50.9, 40.5, 40.5, 30.2, 20.1, 10.0};
	long len2 = 9;
	double val2 = 40.5;
	long index2 = lowest_above_or_equals_test(arr2, len2, val2);
	if (index2 == 4) pass("lowest_above(arr2, len2, val2) == 4");
	else failure("expected 4, got %llu", index2);
	double arr[] = {9, 8, 7, 6, 5, 4, 3, 2, 1};
	long index = lowest_above_or_equals_test(arr, 9, 5.5);
	if (index == 3) pass("lowest_above(arr, len, val) == 3");
	else failure("expected 3, got %llu", index);
	double arr3[] = {1, 1, 1, 1, 1, 1, 1, 1, 1};
	long index3 = lowest_above_or_equals_test(arr3, 9, 1);
	if (index3 == 0) pass("lowest_above(arr3, len3, val3) == 0");
	else failure("expected 0, got %llu", index3);
	double arr4[] = {1};
	long index4 = lowest_above_or_equals_test(arr3, 1, 1);
	if (index4 == 0) pass("lowest_above(arr3, len3, val3) == 0");
	else failure("expected 0, got %llu", index4);
	double arr5[] = {};
	long index5 = lowest_above_or_equals_test(arr3, 0, 1);
	if (index5 == -1) pass("lowest_above(arr3, len3, val3) == -1");
	else failure("expected -1, got %llu", index5);
	double arr6[] = {10, 9, 8, 7, 6, 5, 4, 3, 2, 1};
	long index6 = lowest_above_or_equals_test(arr6, 10, 10);
	if (index6 == 0) pass("lowest_above(arr6, len6, val6) == 0");
	else failure("expected 0, got %llu", index6);
	long index7 = lowest_above_or_equals_test(arr6, 10, 0);
	if (index7 == 9) pass("lowest_above(arr6, len6, val7) == 9");
	else failure("expected 9, got %llu", index7);
	long index8 = lowest_above_or_equals_test(arr6, 10, -100);
	if (index8 == 9) pass("lowest_above(arr6, len6, val8) == 9");
	else failure("expected 9, got %llu", index8);
	double arr7[] = {3, 3, 3, 2, 2, 2, 1, 1, 1};
	long index9 = lowest_above_or_equals_test(arr7, 9, 2);
	if (index9 == 3) pass("lowest_above(arr7, len7, val9) == 3");
	else failure("expected 3, got %llu", index9);
	long index10 = lowest_above_or_equals_test(arr7, 9, 1);
	if (index10 == 6) pass("lowest_above(arr7, len7, val10) == 6");
	else failure("expected 6, got %llu", index10);
	long index11 = lowest_above_or_equals_test(arr7, 9, 4);
	if (index11 == -1) pass("lowest_above(arr7, len7, val11) == -1");
	else failure("expected -1, got %llu", index11);
	long index12 = lowest_above_or_equals_test(arr7, 9, 3.5);
	if (index12 == -1) pass("lowest_above(arr7, len7, val12) == -1");
	else failure("expected -1, got %llu", index12);
	long index13 = lowest_above_or_equals_test(arr7, 9, 0);
	if (index13 == 6) pass("lowest_above(arr7, len7, val13) == 6");
	else failure("expected 6, got %llu", index13);
	long index14 = lowest_above_or_equals_test(arr7, 9, 2.5);
	if (index14 == 0) pass("lowest_above(arr7, len7, val14) == 0");
	else failure("expected 0, got %llu", index14);
	long index15 = lowest_above_or_equals_test(arr7, 9, 1.5);
	if (index15 == 3) pass("lowest_above(arr7, len7, val15) == 3");
	else failure("expected 3, got %llu", index15);
}

void test_min_rel_priori_dcg_v3() {
	double arr2[] = {
		0.99,
		0.89,
		0.79,
		0.69,
		0.59,
		0.49,
		0.39,
		0.29,
		0.19,
		0.09};
	double *thresholds = (double*) malloc(10 * sizeof(double));
	thresholds[0] = 1.00;
	thresholds[1] = 0.98;
	thresholds[2] = 0.90;
	thresholds[3] = 0.85;
	thresholds[4] = 0.75;
	thresholds[5] = 0.7;
	thresholds[6] = 0.65;
	thresholds[7] = 0.6;
	thresholds[8] = 0.55;
	for (int ti = 0; ti < 9; ++ti) {
		double t = thresholds[ti];
		for (int i = 1; i <= 10; ++i) {
			double a = min_rel_priori_dcg_deoptimised_test(arr2, i, t);
			double b = min_rel_priori_dcg_test(arr2, i, t);
			if (a != b) {
				failure(
					"min_rel_priori_dcg_deoptimised(arr2, %d, %f) = %f, "
					"min_rel_priori_dcg(arr2, %d, %f) = %f",
					i, t, a, i, t, b);
			}
			else pass(
				"min_rel_priori_dcg_deoptimised(arr2, %d, %f) = %f, "
				"min_rel_priori_dcg(arr2, %d, %f) = %f",
				i, t, a, i, t, b);
		}
	}
	arr2[0] = 0.5;
	arr2[1] = 0.5;
	arr2[2] = 0.5;
	arr2[3] = 0.5;
	arr2[4] = 0.5;
	arr2[5] = 0.5;
	for (int ti = 0; ti < 9; ++ti) {
		double t = thresholds[ti];
		for (long i = 1; i <= 10; ++i) {
			double a = min_rel_priori_dcg_deoptimised_test(arr2, i, t);
			double b = min_rel_priori_dcg_test(arr2, i, t);
			if (a != b) {
				failure(
					"min_rel_priori_dcg_deoptimised(arr2, %lld, %f) = %f, "
					"min_rel_priori_dcg(arr2, %lld, %f) = %f",
					i, t, a, i, t, b);
			}
			else {
				pass(
					"min_rel_priori_dcg_deoptimised(arr2, %lld, %f) = %f, min_rel_priori_dcg(arr2, %lld, %f) = %f",
					i, t, a, i, t, b);
			}
		}
	}
	arr2[0] = 1;
	arr2[1] = 0.9;
	arr2[2] = 0.88;
	arr2[3] = 0.85;
	arr2[4] = 0.82;
	arr2[5] = 0.8;
	arr2[6] = 0.78;
	arr2[7] = 0.75;
	arr2[8] = 0.72;
	arr2[9] = 0.7;
	double a = min_rel_priori_dcg_deoptimised_test(arr2, 10, 1);
	double b = min_rel_priori_dcg_test(arr2, 10, 1);
	if (a != b) {
		failure(
			"min_rel_priori_dcg_deoptimised(arr2, %d, %f) = %f, "
			"min_rel_priori_dcg(arr2, %d, %f) = %f",
			10, 1.0, a, 10, 1.0, b);
	}
	else pass(
		"min_rel_priori_dcg_deoptimised(arr2, %d, %f) = %f, "
		"min_rel_priori_dcg(arr2, %d, %f) = %f",
		10, 1.0, a, 10, 1.0, b);
}

int main() {
	test_attention_models();
	test_lowest_above_or_equals();
	test_min_rel_priori_dcg_v3();
}
