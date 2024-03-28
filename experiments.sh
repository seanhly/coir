#!/usr/bin/env bash
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m'
mkdir -p data/00_csvs
mkdir -p data/02_dense_input_bin
mkdir -p data/03_model_bin
mkdir -p data/04_relevance_bin

heading(){
	echo -e "$GREEN# $@$NC"
}

notice(){
	# Check if arg1 is numeric-like using regex
	if [[ "$1" =~ ^[0-9]+$ ]]; then
		indent=$1
		while [ $indent -gt 0 ]; do
			echo -e -n '\t'
			indent=$((indent - 1))
		done
	fi
	shift
	echo -e "$YELLOW$@$NC"
}

heading Converting CSVs to binary
for src in data/00_csvs/aol_clicks.csv.zst data/00_csvs/sogouq_clicks.csv.zst; do
	basename="$(basename $src)"
	dst="data/01_sparse_input_bin/${basename%_clicks.csv.zst}.bin.zst"
	echo -e '\t'"$src --> $dst"
	if [ "$dst" -nt "$src" ]; then
		notice 1 Already up to date.
	else
		pv "$src" | zstdcat | ./bin/librecoir convert-sparse-click-log > "$dst"
	fi
done
Y_CLICKS="data/00_csvs/yandex_clicks.csv.zst"
Y_RANKS="data/00_csvs/yandex_ranks.csv.zst"
Y_BIN="data/02_dense_input_bin/yandex.bin.zst"
heading Converting Yandex click logs to binary
echo -e '\t'"$Y_CLICKS + $Y_RANKS --> $Y_BIN"
if \
	[ "$Y_BIN" -nt "$Y_CLICKS" ] &&
	[ "$Y_BIN" -nt "$Y_RANKS" ] &&
	[ -s "$Y_BIN" ]; then
	notice 1 Already up to date.
else
	./bin/librecoir convert-dense-click-log "$Y_CLICKS" "$Y_RANKS" > "$Y_BIN"
fi

heading Converting sparse click logs to dense
for src in data/01_sparse_input_bin/sogouq.bin.zst data/01_sparse_input_bin/aol.bin.zst; do
	basename="$(basename $src)"
	dst="data/02_dense_input_bin/$basename"
	echo -e '\t'"$src --> $dst"
	if [ "$dst" -nt "$src" ]; then
		notice 1 Already up to date.
	else
		pv "$src" | zstdcat | ./bin/librecoir click-log-predict-rankings | zstd > "$dst"
	fi
done

heading Calculating smaller models
for src in data/02_dense_input_bin/sogouq.bin.zst data/02_dense_input_bin/aol.bin.zst; do
	basename="$(basename $src)"
	label="${basename%.bin.zst}"
	echo -e '\t'"$src --> data/03_model_bin/*$label*"
	# Check if any file in data/03_model_bin is older than src
	# or if there are no files in data/03_model_bin
	old=0
	if [ ! -e data/03_model_bin ]; then
		old=1
	else
		for model in data/03_model_bin/*$label*; do
			if [ "$model" -ot "$src" ]; then
				old=1
				break
			fi
		done
	fi
	if [ $old -eq 0 ]; then
		notice 1 Already up to date.
	else
		pv "$src" \
			| zstdcat \
			| ./bin/librecoir pscm data/03_model_bin "$label"
	fi
done

heading Applying confidence intervals
for src in data/03_model_bin/*; do
	basename="$(basename $src)"
	params="${basename%.bin.zst}"
	rounds="${params##*_}"
	label_method="${params%_*}"
	label="${label_method%_*}"
	model="${label_method#*_}"
	echo -e '\t'"$src --> data/03_model_bin/*$label*"
	tag="${label}_${method}_${rounds}_rounds_${confidence}_confidence.bin.zst"
	dst="data/04_relevance_bin/$tag"
	for c in $(jq '.["confidence-levels"][]' < experiment-params.json); do
		notice 1 Calculating doc-query relevances with confidence $c
		zstdcat "$src" \
			| ./bin/librecoir confident-min-relevance 10 2>/dev/null \
			| zstd > "$tag"
	done
done
