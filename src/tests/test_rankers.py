from triage.rankers import rank_predictions
from tests.results_tests.factories import ModelFactory, PredictionFactory, session as factory_session
import datetime
from sqlalchemy.orm import Session
from triage.component.results_schema import schema as results_schema

def test_rank_predictions(db_engine_with_results_schema):
    dates = [datetime.date(2016, 1, 1), datetime.date(2016, 6, 1)]
    models = [ModelFactory(model_id=model_id) for model_id in range(1, 5)]
    for date in dates:
        for entity_id in range(1, 10):
            for model in models:
                PredictionFactory(
                    as_of_date=date,
                    entity_id=entity_id,
                    model_rel=model,
                    rank_abs=None,
                    rank_pct=None
                )
    factory_session.commit()
    rank_predictions(db_engine_with_results_schema, 1, dates[0], "test_results.predictions", "dense")

    for prediction in Session(bind=db_engine_with_results_schema)\
            .query(results_schema.TestPrediction)\
            .filter_by(model_id=1, as_of_date=dates[0]):
        assert prediction.rank_abs
        assert prediction.rank_pct
