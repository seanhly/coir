#!/usr/bin/env swipl

% This is necessary for SWI-Prolog scripts run as standalone programs.
:- initialization(main, main).

main :-
    read_string(user_input, "", "", _, Input),
    string_chars(Input, Chars),
    candidate_file(X, Chars, []),
    print(X),
    nl,
    print(Y),
    nl,
	\+find_best(
		better_solution,
		quality_re_ranking,
		[X, _, 1] % 1] %0.90]
	),
	solution_count(C),
	write(C), nl,
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

find_best(IsBetter, Functor, Template) :-
	unify(Template, FirstArgs),
	unify(Template, Args),
	FirstPred =.. [Functor|FirstArgs],
	OtherPred =.. [Functor|Args],
	FirstPred,
	assertz(best_solution(FirstArgs)),
	assertz(solution_count(1)),
	retractall(seen_candidate(_, possible_solution)),
	!,
	%guitracer, trace, 
	OtherPred,
	(
		solution_count(C),
		NextCount is C + 1,
		%print100(NextCount),
		retractall(solution_count(C)),
		assertz(solution_count(NextCount))
	),
	(
		best_solution(PreviousBest),
		IsBetterPred =.. [IsBetter,Args,PreviousBest],
		(
			IsBetterPred,
			retractall(best_solution(PreviousBest)),
			assertz(best_solution(Args))
			;
			\+IsBetterPred
		)
	),
	fail.

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

quality_re_ranking(
	Candidates,
	ReRanking,
	QualityThreshold
) :-
    AttSum is 0,
    rel_sum(Candidates, RelSum),
    print([rel,RelSum]), nl,
    best_ranking_dcg(1, Candidates, 0, IDCG),
    MinDCG is IDCG * QualityThreshold,
    print(MinDCG), nl,
    !,
    quality_re_ranking_dcg(1, Candidates, ReRanking, MinDCG)
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


quality_re_ranking_dcg(Rank, [], [], _).
quality_re_ranking_dcg(Rank, [H|T], [DocID:Rel:PrevExposure|NextRanks], MinDCG) :-
	any_unseen_candidate(H, DocID:Rel, possible_solution),
	gain(Rank, Rel, Gain),
	NextRank is Rank + 1,
	assertz(seen_candidate(DocID, dcg)),
	best_ranking_dcg(NextRank, T, Gain, DCG), % also retracts the above.
	DCG >= MinDCG,
	(
		assertz(seen_candidate(DocID, possible_solution));
		retract(seen_candidate(DocID, possible_solution)), fail
	),
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

best_ranking_dcg(_, [], DCG, DCG) :-
	retractall(seen_candidate(_, dcg)).
best_ranking_dcg(Rank, [H|T], RunningDCG, DCG) :-
	best_unseen_candidate(H, DocID:Rel, dcg),
	assertz(seen_candidate(DocID, dcg)),
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
