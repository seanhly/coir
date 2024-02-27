void free_rank_contexts(Context ctx) {
    for (int i = 0; i < ctx.rank_contexts_c; i++)
        free(ctx.rank_contexts[i].candidates_ptr);
    free(ctx.rank_contexts);
    free(ctx.rank_contexts_per_rank);
}

void free_context(Context ctx) {
    free_rank_contexts(ctx);
    free(ctx.candidates_ptr);
}
