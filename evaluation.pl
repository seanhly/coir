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
    candidate_file(Candidates, Chars, []),
    print(Candidates),
    nl,
	aggregate_all(
		min(Unfairness, ReRanking),
		quality_re_ranking(Candidates, ReRanking, T, Unfairness),
		min(_, ReRanking)
	),
	write(ReRanking), nl,
    nl,
    halt.

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

dcg(L, DCG) :-
	dcg(1, L, 0, DCG), !.

gain(Rank, Rel, Gain) :-
	Gain is log(2) * (2**Rel - 1) / log(Rank + 1).

dcg(Rank, [_:Rel:_|Remainder], RunningDCG, DCG) :-
	gain(Rank, Rel, Gain),
	NextDCG is RunningDCG + Gain,
	NextRank is Rank + 1,
	dcg(NextRank, Remainder, NextDCG, DCG), !.
dcg(_, [], X, X) :- !.

bulk_if_metrics([], R, E, R, E).
bulk_if_metrics([_:Relevance:Exposure|T], RunningR, RunningE, EndR, EndE) :-
	NextR is RunningR + Relevance,
	NextE is RunningE + Exposure,
	bulk_if_metrics(T, NextR, NextE, EndR, EndE).
negative_to_0(U, PU) :- U < 0 -> PU = 0 ; PU = U.
item_unfairness(Relevance, Exposure, TotalRelevance, TotalExposure, Unfairness) :-
	SignedItemUnfairness is TotalExposure / TotalRelevance - Exposure / Relevance,
	negative_to_0(SignedItemUnfairness, Unfairness).
individual_unfairness([], _, _, Unfairness, Unfairness).
individual_unfairness([_:R:E|Tail], TotalR, TotalE, RunningU, Unfairness) :-
	item_unfairness(R, E, TotalR, TotalE, IU),
	NextU is RunningU + IU,
	individual_unfairness(Tail, TotalR, TotalE, NextU, Unfairness).
individual_unfairness(Ranking, Unfairness) :-
	bulk_if_metrics(Ranking, 0, 0, TotalRelevance, TotalExposure),
	individual_unfairness(Ranking, TotalRelevance, TotalExposure, 0, Unfairness).
	

quality_re_ranking(
	Candidates,
	ReRanking,
	QualityThreshold,
	Unfairness
) :-
    rel_sum(Candidates, RelSum),
    print([rel,RelSum]), nl,
    best_ranking_dcg(1, Candidates, 0, IDCG),
    MinDCG is IDCG * QualityThreshold,
    print(MinDCG), nl,
    !,
    quality_re_ranking_dcg(1, Candidates, ReRanking, MinDCG),
	individual_unfairness(ReRanking, Unfairness)
    % , print(ReRanking), nl
    .

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
	print(DocRels), nl,
	findall(Rel, member(_:Rel, DocRels), Rels),
	sum_list(Rels, RelSum).


quality_re_ranking_dcg(_, [], [], _).
quality_re_ranking_dcg(Rank, [H|T], [DocID:Rel:_|NextRanks], MinDCG) :-
	any_unseen_candidate(H, DocID:Rel, possible_solution),
	gain(Rank, Rel, Gain),
	NextRank is Rank + 1,
	best_ranking_dcg(NextRank, T, Gain, DCG),
	DCG >= MinDCG,
	NextMinDCG is MinDCG - Gain,
	quality_re_ranking_dcg(NextRank, T, NextRanks, NextMinDCG).

unseen_candidate(_, _) :-
	\+current_predicate(seen_candidate/2), !.
unseen_candidate(DocID, Scope) :-
	\+seen_candidate(DocID, Scope), !.

best_unseen_candidate([DocID:Rel:_|_], DocID:Rel, Scope) :-
	unseen_candidate(DocID, Scope),
	unseen_candidate(DocID, possible_solution),
	!.
best_unseen_candidate([_:_:_|T], C, Scope) :-
	best_unseen_candidate(T, C, Scope).

best_ranking_dcg(_, [], DCG, DCG).
best_ranking_dcg(Rank, [H|T], RunningDCG, DCG) :-
	best_unseen_candidate(H, _:Rel, dcg),
	NextRank is Rank + 1,
	gain(Rank, Rel, Gain),
	NextRunningDCG is RunningDCG + Gain,
	best_ranking_dcg(NextRank, T, NextRunningDCG, DCG).

any_unseen_candidate(L, DocID:Rel, Scope) :-
	member(DocID:Rel:_, L), unseen_candidate(DocID, Scope).

candidate_file([L|T]) -->
	candidate_file_line(H),
	{ predsort(compare_relevance, H, L) },
	candidate_file(T).
candidate_file([]) --> [].

compare_relevance(<, _:A:_, _:B:_) :- A > B, !.
compare_relevance(>, _:A:_, _:B:_) :- A < B, !.
compare_relevance(=, A:A:_, A:A:_) :- !.
compare_relevance(<, B:A:_, C:A:_) :- B @> C, !.
compare_relevance(>, B:A:_, C:A:_) :- B @< C, !.

candidate_file_line([H|T]) -->
	candidate_record(H),
	['\t'],
	candidate_file_line(T).

candidate_file_line([H]) -->
	candidate_record(H),
	['\n'].

candidate_record(DocID:Relevance:0) -->
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

doc_id([H|T]) --> [H],
	{ \+member(H, ['\t', '\n']) }, !,
	doc_id(T).
doc_id([]) --> [].

relevance([H|T]) --> [H], { \+member(H, ['\t', '\n']) }, !, relevance(T).
relevance([]) --> [].

pad_decimal([H|T], Padded) :-
	H = '.' -> Padded = ['0',H|T] ; Padded = [H|T].

% vim: set ft=prolog :
