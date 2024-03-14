#!/usr/bin/env swipl

% Necessary for aggregate_all.
:- use_module(library(aggregate)).

% This is necessary for SWI-Prolog scripts run as standalone programs.
:- initialization(main, main).

get_threshold_from_arg([Arg|_], T) :-
	!, atom_number(Arg, T).
get_threshold_from_arg([], 0.99).

main :-
	current_prolog_flag(argv, Argv),
	get_threshold_from_arg(Argv, T),
    read_string(user_input, "", "", _, Input),
    string_chars(Input, Chars),
    candidate_file(Relevancies, Candidates, Chars, []),
	aggregate_all(
		min(Unfairness, [ReRanking, UpdatedCandidates]),
		quality_re_ranking(
			Candidates, Relevancies, ReRanking, T,
			Unfairness, UpdatedCandidates),
		min(_, [ReRanking, UpdatedCandidates])
	),
	write(ReRanking), nl,
	aggregate_all(
		min(Unfairness2, [ReRanking2, UpdatedCandidates2]),
		quality_re_ranking(
			UpdatedCandidates, Relevancies, ReRanking2, T,
			Unfairness2, UpdatedCandidates2),
		min(_, [ReRanking2, UpdatedCandidates2])
	),
	write(ReRanking2), nl,
	aggregate_all(
		min(Unfairness3, [ReRanking3, UpdatedCandidates3]),
		quality_re_ranking(
			UpdatedCandidates2, Relevancies, ReRanking3, T,
			Unfairness3, UpdatedCandidates3),
		min(_, [ReRanking3, UpdatedCandidates3])
	),
	write(ReRanking3), nl
	.

unify(A, B) :-
	length(A, L),
	length(B, L),
	unify_nonvars(A, B), !.
unify_nonvars([], []) :- !.
unify_nonvars([H|T1], [_|T2]) :-
	var(H),
	unify_nonvars(T1, T2), !.
unify_nonvars([H1|T1], [H2|T2]) :-
	nonvar(H1),
	H1 = H2,
	unify_nonvars(T1, T2), !.

better_solution([_,L1,_],[_,L2,_]) :-
	length(L1, Len1),
	length(L2, Len2),
	Len1 > Len2.

print100(X) :- 0 is X mod 1000, print(X), nl, !.
print100(_).

dcg(L, R, DCG) :-
	dcg(1, R, L, 0, DCG), !.

gain(Rank, Rel, Gain) :-
	Gain is log(2) * (2**Rel - 1) / log(Rank + 1).

dcg(Rank, Relevancies, [DocID:_|Remainder], RunningDCG, DCG) :-
	Rel = Relevancies.get(DocID),
	gain(Rank, Rel, Gain),
	NextDCG is RunningDCG + Gain,
	NextRank is Rank + 1,
	dcg(NextRank, Remainder, NextDCG, DCG), !.
dcg(_, _, [], X, X) :- !.

bulk_if_metrics(_, [], R, E, R, E).
bulk_if_metrics(Relevancies, [D:E|T], RunningR, RunningE, EndR, EndE) :-
	R = Relevancies.get(D),
	NextR is RunningR + R,
	NextE is RunningE + E,
	bulk_if_metrics(Relevancies, T, NextR, NextE, EndR, EndE).
negative_to_0(U, PU) :- U < 0 -> PU = 0 ; PU = U.
item_unfairness(
	Relevance, Exposure, TotalRelevance,
	TotalExposure, Unfairness
) :-
	ExpectedRelativeCompensation is
	    Relevance / TotalRelevance,
	RelativeCompensation is Exposure / TotalExposure,
	FlatUnfairness =
		ExpectedRelativeCompensation - RelativeCompensation,
	Unfairness is FlatUnfairness ** 2, !.
individual_unfairness(_, [], _, _, Unfairness, Unfairness).
individual_unfairness(
	Relevancies,
	[DocID:E|Tail], TotalR, TotalE, RunningU, Unfairness
) :-
	R = Relevancies.get(DocID),
	item_unfairness(R, E, TotalR, TotalE, IU),
	NextU is RunningU + IU,
	individual_unfairness(
		Relevancies, Tail, TotalR, TotalE, NextU, Unfairness).
individual_unfairness(Relevancies, Candidates, Unfairness) :-
	bulk_if_metrics(
		Relevancies, Candidates, 0, 0,
		TotalRelevance, TotalExposure),
	individual_unfairness(
		Relevancies, Candidates,
		TotalRelevance, TotalExposure, 0, Unfairness),
	!.

geometric_attention(Rank, A) :- A is (1 - 0.4) ** (Rank - 1).

update_exposure_in_rank_candidates(_, [], []).
update_exposure_in_rank_candidates(
	UpdatedReRanking, [D:EIn|TIn], [D:EOut|TOut]
) :-
	(member(D:EOut, UpdatedReRanking) ; EOut = EIn),
	!,
	update_exposure_in_rank_candidates(UpdatedReRanking, TIn, TOut).
update_exposure_in_candidates(_, [], []).
update_exposure_in_candidates(
	UpdatedReRanking,
	[RankCandidates|T1],
	[UpdatedRankCandidates|T2]
) :-
	update_exposure_in_rank_candidates(
		UpdatedReRanking, RankCandidates, UpdatedRankCandidates),
	update_exposure_in_candidates(UpdatedReRanking, T1, T2).
update_exposure_inner(_, [], []).
update_exposure_inner(
	Rank, [D:EIn|TIn], [D:EOut|TOut]
) :-
	geometric_attention(Rank, A),
	EOut is EIn + A,
	NextRank is Rank + 1,
	update_exposure_inner(NextRank, TIn, TOut).
update_exposure(
	ReRanking, Candidates, UpdatedCandidates
) :-
	update_exposure_inner(1, ReRanking, UpdatedReRanking),
	update_exposure_in_candidates(
		UpdatedReRanking, Candidates, UpdatedCandidates), !.

all_candidates(Candidates, AllCandidates) :-
	append(_, [AllCandidates], Candidates), !.

quality_re_ranking(
	Candidates,
	Relevancies,
	ReRanking,
	QualityThreshold,
	Unfairness,
	UpdatedCandidates
) :-
    best_ranking_dcg(Relevancies, 1, Candidates, 0, IDCG, seen{}),
    MinDCG is IDCG * QualityThreshold,
    !,
    quality_re_ranking_dcg(
		1, Relevancies, Candidates, ReRanking, MinDCG, seen{}),
    update_exposure(
		ReRanking, Candidates, UpdatedCandidates),
	all_candidates(UpdatedCandidates, AllCandidates),
	individual_unfairness(
		Relevancies, AllCandidates, Unfairness).

rel_sum(Candidates, RelSum) :-
	findall(
		DocID:Rel,
		(
			member(L, Candidates),
			member(DocID:Rel:_, L)
		),
		Dupes
	),
	setof(M, member(M, Dupes), DocRels),
	findall(Rel, member(_:Rel, DocRels), Rels),
	sum_list(Rels, RelSum).


quality_re_ranking_dcg(_, _, [], [], _, _).
quality_re_ranking_dcg(
	Rank,
	Relevancies,
	[H|T],
	[DocID:Exposure|NextRanks],
	MinDCG,
	Seen
) :-
	any_unseen_candidate(H, DocID:Exposure, Seen),
	Rel = Relevancies.get(DocID),
	NextSeen = Seen.put(DocID, true),
	gain(Rank, Rel, Gain),
	NextRank is Rank + 1,
	best_ranking_dcg(Relevancies, NextRank, T, Gain, DCG, seen{}),
	DCG >= MinDCG,
	NextMinDCG is MinDCG - Gain,
	quality_re_ranking_dcg(
		NextRank, Relevancies, T, NextRanks, NextMinDCG, NextSeen).

unseen_candidate(DocID, Seen) :-
	\+(_ = Seen.get(DocID)).

best_unseen_candidate([DocID:_|_], DocID, Seen) :-
	unseen_candidate(DocID, Seen),
	!.
best_unseen_candidate([_:_|T], C, Seen) :-
	best_unseen_candidate(T, C, Seen).

best_ranking_dcg(_, _, [], DCG, DCG, _).
best_ranking_dcg(Relevancies, Rank, [H|T], RunningDCG, DCG, Seen) :-
	best_unseen_candidate(H, DocID, Seen),
	Rel = Relevancies.get(DocID),
	NextRank is Rank + 1,
	gain(Rank, Rel, Gain),
	NextSeen = Seen.put(DocID, true),
	NextRunningDCG is RunningDCG + Gain,
	best_ranking_dcg(Relevancies, NextRank, T, NextRunningDCG, DCG, NextSeen).

any_unseen_candidate(L, DocID:Exposure, Seen) :-
	member(DocID:Exposure, L), unseen_candidate(DocID, Seen).

strip_relevance([], []).
strip_relevance([D:_:E|T], [D:E|T1]) :- strip_relevance(T, T1).

compare_relevance(<, _:A:_, _:B:_) :- A > B, !.
compare_relevance(>, _:A:_, _:B:_) :- A < B, !.
compare_relevance(=, A:A:_, A:A:_) :- !.
compare_relevance(<, B:A:_, C:A:_) :- B @> C, !.
compare_relevance(>, B:A:_, C:A:_) :- B @< C, !.

candidate_rank_lines(_, []) --> [].

candidate_rank_lines(Relevancies, [L1|T]) -->
	candidate_rank_line(Relevancies, H),
	{
		predsort(compare_relevance, H, L),
		strip_relevance(L, L1)
	},
	candidate_rank_lines(Relevancies, T).

candidate_relevancy(DocID:Relevance) -->
	doc_id(D),
	{
		atomic_list_concat(D, DocID)
	},
	['\t'],
	relevance(R),
	{
		pad_decimal(R, PaddedRelString),
		atomic_list_concat(PaddedRelString, RelAtom),
		atom_number(RelAtom, Relevance)
	}.

candidate_relevancies(Rel) -->
	candidate_relevancy(D:R),
	{ Rel = rel{}.put(D, R) },
	['\n'].

candidate_relevancies(NextRels) -->
	candidate_relevancy(D:R),
	['\t'],
	candidate_relevancies(PrevRels),
	{ NextRels = PrevRels.put(D, R) }.

candidate_file(Relevancies, RankLines) -->
	candidate_relevancies(Relevancies),
	candidate_rank_lines(Relevancies, RankLines).

candidate_rank_line(Relevancies, [H|T]) -->
	candidate_record(Relevancies, H),
	['\t'],
	candidate_rank_line(Relevancies, T).

candidate_rank_line(Relevancies, [H]) -->
	candidate_record(Relevancies, H),
	['\n'].

candidate_record(Relevancies, DocID:Relevance:0) -->
	doc_id(D),
	{
		atomic_list_concat(D, DocID),
		Relevance = Relevancies.get(DocID)
	}.

doc_id([H|T]) --> [H],
	{ \+member(H, ['\t', '\n']) }, !,
	doc_id(T).
doc_id([]) --> [].

relevance([H|T]) --> [H], { \+member(H, ['\t', '\n']) }, !, relevance(T).
relevance([]) --> [].

pad_decimal([H|T], Padded) :-
	H = '.' -> Padded = ['0',H|T] ; Padded = [H|T].

% vim: set ft=prolog :
