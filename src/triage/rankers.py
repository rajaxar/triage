import logging
import random
import pandas

def everyone_is_best(df, pct=False):
    return pandas.Series([1] * len(df))

CUSTOM_RANKING_LOOKUP = {
    'everyone_is_best': everyone_is_best
}

def rank_predictions_by_method(df, method, pct, **kwargs):
    """Takes in a df that will have columns entity_id, score, label_value

    Returns a series with the configured rank
    """
    try:
        return df['score'].rank(method=method, pct=pct, ascending=False, **kwargs)
    except KeyError as exc:
        logging.info("pandas rank attempt errored with %s. Trying custom rankers", exc)
        ranking_func = CUSTOM_RANKING_LOOKUP.get(method, None)
        if not ranking_func:
            raise ValueError("Ranking method %s not compatible with pandas or custom ranking lookup", method)
        return ranking_func(df, pct=pct, **kwargs)


def compute_ranking_dataframe(db_engine, model_id, as_of_date, full_predictions_table_name):
    return pandas.read_sql(
        f"select entity_id, score, label_value from {full_predictions_table_name} where model_id = %(model_id)s and as_of_date = %(as_of_date)s",
        params={'model_id': model_id, 'as_of_date': as_of_date},
        con=db_engine
    )


def upsert_prediction_ranks(db_engine, ranked_df, full_predictions_table_name):
    ranked_df.to_sql("temp", con=db_engine)
    db_engine.execute(f"update {full_predictions_table_name} as preds set rank_abs = t.rank_abs, rank_pct = t.rank_pct from temp as t where t.model_id = preds.model_id and t.as_of_date = preds.as_of_date and t.entity_id = preds.entity_id; drop table temp")



def rank_predictions(db_engine, model_id, as_of_date, full_predictions_table_name, method, **rank_kwargs):
    unranked_df = compute_ranking_dataframe(db_engine, model_id, as_of_date, full_predictions_table_name)
    abs_ranks = rank_predictions_by_method(unranked_df, method, pct=False, **rank_kwargs)
    pct_ranks = rank_predictions_by_method(unranked_df, method, pct=True, **rank_kwargs)
    ranked_df = unranked_df.copy()
    ranked_df['rank_abs'] = abs_ranks
    ranked_df['rank_pct'] = pct_ranks
    ranked_df['model_id'] = model_id
    ranked_df['as_of_date'] = as_of_date

    upsert_prediction_ranks(db_engine, ranked_df, full_predictions_table_name)
