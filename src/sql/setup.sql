\echo 'Triage'
\echo 'Adolfo De Unánue <adolfo@unanue.mx>'
\set VERBOSITY terse
\set ON_ERROR_STOP true

do language plpgsql $$ declare
    exc_message text;
    exc_context text;
    exc_detail text;
begin

raise notice 'dropping schemas';
drop schema if exists triage_metadata cascade;
drop schema if exists test_results cascade;
drop schema if exists train_results cascade;
drop schema if exists triage_production cascade;

raise notice 'creating schemas';
create schema if not exists triage_metadata;
create schema if not exists test_results;
create schema if not exists train_results;
create schema if not exists triage_production;

raise notice 'populating schema triage_metadata';
do $triage_metadata$ begin
    set search_path = triage_metadata, public;

    drop type if exists triage_run_status;
    create type triage_run_status as enum ('started', 'completed', 'failed');

    drop type if exists matrix_type;
    create type matrix_type as enum ('train', 'test');

    drop type if exists tiebreaker_ordering;
    create type tiebreaker_ordering as enum ('best', 'worst', 'random');

    drop type if exists metric;
    create type metric as enum (
           'precision@'
           , 'recall@'
           , 'fbeta@'
           , 'f1'
           , 'accuracy'
           , 'roc_auc'
           , 'average precision score'
           , 'true positives@'
           , 'true negatives@'
           , 'false positives@'
           , 'false negatives@'
           , 'fpr@'
    );

    drop table if exists experiments;
    create table if not exists experiments (
               experiment_id                integer generated always as identity primary key
               , experiment_hash            text unique
               , config                     jsonb
               , time_splits                smallint
               , as_of_times                smallint
               , total_features             smallint
               , feature_group_combinations smallint
               , matrices_needed            smallint
               , grid_size                  smallint
               , models_needed              smallint
    );

    drop table if exists retrains;
    create table if not exists retrains (
        retrain_id        integer generated always as identity primary key
        , retrain_hash      text unique
        , config          jsonb
        , prediction_date timestamp with time zone
        , created_at      timestamp with time zone default now()
    );

    drop table if exists triage_runs;
    create table if not exists triage_runs (
             triage_run_id         integer generated always as identity primary key
                , start_time       timestamp with time zone
                , start_method     text
                , git_hash         text
                , python_version   text
                , run_type         text
                , run_hash         text
                , platform         text
                , os_user          text
                , working_directory text
                , ec2_instance_type text
                , log_location      text
                , experiment_class_path text
                , experiment_kwargs     jsonb
                , installed_libraries   text[]
                , matrix_building_started timestamp with time zone not null default now()
                , matrices_made           smallint default 0
                , matrices_skipped        smallint default 0
                , matrices_errored        smallint default 0
                , model_building_started  timestamp with time zone
                , models_made           smallint default 0
                , models_skipped        smallint default 0
                , models_errored        smallint default 0
                , last_updated_time     timestamp with time zone not null default now()
                , current_status        triage_run_status
                , stacktrace            text
                , random_seed           integer
                , cohort_table_name     text
                , labels_table_name     text
                , bias_hash             text
    ) ;

    drop table if exists subsets;
    create table if not exists subsets (
        subset_id        integer generated always as identity primary key
        , subset_hash    text unique
        , config         jsonb
        , created_at     timestamp with time zone not null default now()
    );


    drop table if exists model_groups;
    create table if not exists model_groups (
               model_group_id     integer generated always as identity primary key
               , model_type       text
               , hyperparameters  jsonb
               , feature_list     text[]
               , model_config     jsonb
    );


    drop table if exists matrices;
    create table if not exists matrices (
                matrix_id            integer generated always as identity primary key
                , experiment_id      integer references experiments (experiment_id)  -- was built_by_experiment
                , matrix_hash        text unique                                     -- was matrix_uuid
                , matrix_type        matrix_type
                , labeling_window    tstzrange
                , num_observations   integer
                , creation_time      timestamp with time zone default now()
                , lookback_duration   tstzrange
                , feature_start_time  timestamp with time zone
                , matrix_metadata     jsonb
                , built_by_experiment text
                , feature_dictionary  jsonb
    );

    -- -- alter table  matrices add foreign key (built_by_experiment) references experiments (experiment_hash);

    drop table if exists experiments_matrices;
    create table if not exists experiments_matrices (
        experiment_id      integer references experiments (experiment_id) on update cascade on delete cascade
        , matrix_id        integer references matrices (matrix_id) on update cascade
        , primary key (experiment_id, matrix_id)
    );

    -- --     -- alter table  experiment_matrices add foreign key (experiment_hash) references experiments (experiment_hash);
    -- --     -- alter table  experiment_matrices add foreign key (matrix_hash) references matrices (matrix_hash);

    drop table if exists models;
    create table if not exists models (
               model_id           integer generated always as identity primary key
               , model_group_id   integer references model_groups(model_group_id)
               , model_hash       text unique
               , run_time         timestamp with time zone
               , batch_run_time   timestamp with time zone
               , model_type       text
               , hyperparameters  jsonb
               , model_comment    text
               , batch_comment    text
               , config           jsonb
               , built_in_triage_run  integer references triage_runs (triage_run_id)
               , train_end_time       timestamp with time zone
               , test                 boolean
               , trained_on_matrix    integer references matrices (matrix_id)   -- was train_matrix_hash
               , model_size           real
               , random_seed          integer
    );

    --     -- alter table  models add foreign key (model_group_id) references model_groups (model_group_id);
    --     -- alter table  models add foreign key (built_in_triage_run) references triage_runs (triage_run_id);
    --     -- alter table  models add foreign key (train_matrix_hash) references matrices (matrix_hash);

    drop table if exists experiments_models;
    create table if not exists experiments_models (
        experiment_id      integer references experiments(experiment_id) on update cascade on delete cascade
        , model_id         integer references models(model_id) on update cascade
        , primary key (experiment_id, model_id)
    );

    --     -- alter table  experiment_models add foreign key (experiment_hash) references experiments (experiment_hash);
    --     -- alter table  experiment_models add foreign key (model_hash) references models (model_hash);


    drop table if exists retrains_models;
    create table if not exists retrains_models (
        retrain_id      integer references retrains (retrain_id)
        , model_id      integer references models (model_id)
        , primary key (retrain_id, model_id)
    );

    --     -- alter table  retrain_models add foreign key (model_hash) references models (model_hash);
    --     -- alter table  retrain_models add foreign key (retrain_hash) references retrain (retrain_hash);

end $triage_metadata$;

raise notice 'populating schema train_results';
do $train_results$ begin
    set search_path = train_results, public;


    drop table if exists feature_importances;
    create table if not exists feature_importances (
        feature_importance_id integer generated always as identity
        , model_id            integer references triage_metadata.models (model_id)
        , feature             text
        , feature_importance  real
        , rank_abs            smallint
        , rank_pct            real
        , primary key (model_id, feature_importance_id)
    );

    drop table if exists predictions;
    create table if not exists predictionss (
        model_id              integer references triage_metadata.models (model_id)
        , entity_id           integer
        , as_of_date          timestamp with time zone
        , score               real
        , label_value         smallint
        , rank_abs_no_ties    smallint
        , rank_abs_with_ties  smallint
        , rank_pct_no_ties    real
        , rank_pct_with_ties  real
        , matrix_id           integer references triage_metadata.matrices (matrix_id)
        , test_label_timespan tstzrange
        , primary key (model_id, entity_id, as_of_date)
    );

    drop table if exists prediction_metadata;
    create table if not exists prediction_metadata (
        model_id              integer references triage_metadata.models (model_id)
        , matrix_id           integer references triage_metadata.matrices (matrix_id)
        , tiebreaker_ordering triage_metadata.tiebreaker_ordering
        , random_seed         integer
        , predictions_saved   boolean
        , primary key (model_id, matrix_id)
    );

    drop table if exists evaluations;
    create table if not exists evaluations (
        model_id                  integer references triage_metadata.models (model_id)
        , evaluation_start_time   timestamp with time zone
        , evaluation_end_time     timestamp with time zone
        , as_of_date_frequency    tstzrange
        , subset_id               integer references triage_metadata.subsets (subset_id)
        , metric                  triage_metadata.metric
        , parameter               text
        , matrix_id               integer references triage_metadata.matrices (matrix_id)
        , num_labeled_examples        integer
        , num_labeled_above_threshold integer
        , num_positive_labels         integer
        , sort_seed                   integer
        , best_value                  real
        , worst_value                 real
        , stochastic_value            real
        , num_sort_trials             smallint
        , standard_deviation          real
        , primary key(model_id, evaluation_start_time, evaluation_end_time, as_of_date_frequency, metric, parameter, subset_id)
    );

    drop table if exists aequitas;
    create table if not exists aequitas (
        model_id                  integer references triage_metadata.models (model_id)
        , subset_id               integer references triage_metadata.subsets (subset_id)
        , tie_breaker             triage_metadata.tiebreaker_ordering
        , evaluation_start_time   timestamp with time zone
        , evaluation_end_time     timestamp with time zone
        , as_of_date_frequency    tstzrange
        , parameter          text
        , attribute_name     text
        , attribute_value    text
        , matrix_id          integer not null references triage_metadata.matrices (matrix_id)
        , total_entities     integer
        , group_label_pos    integer
        , group_label_neg    integer
        , group_size         integer
        , group_size_pct     real
        , prev               integer
        , pp                 integer
        , pn                 integer
        , fp                 integer
        , fn                 integer
        , tn                 integer
        , tp                 integer
        , ppr                integer
        , pprev              integer
        , tpr                integer
        , tnr                integer
        , "for"              integer
        , fdr                integer
        , fpr                integer
        , fnr                integer
        , npv                integer
        , precision          real
        , ppr_disparity      real
        , ppr_ref_group_value text
        , pprev_disparity real
        , pprev_ref_group_value text
        , precision_disparity real
        , precision_ref_group_value text
        , fdr_disparity real
        , fdr_ref_group_value text
        , for_disparity real
        , for_ref_group_value text
        , fpr_disparity real
        , fpr_ref_group_value text
        , fnr_disparity real
        , fnr_ref_group_value text
        , tpr_disparity real
        , tpr_ref_group_value text
        , tnr_disparity real
        , tnr_ref_group_value text
        , npv_disparity real
        , npv_ref_group_value text
        , "Statistical_Parity" boolean
        , "Impact_Parity" boolean
        , "FDR_Parity" boolean
        , "FPR_Parity" boolean
        , "FOR_Parity" boolean
        , "FNR_Parity" boolean
        , "TypeI_Parity" boolean
        , "TypeII_Parity" boolean
        , "Equalized_Odds" boolean
        , "Unsupervised_Fairness" boolean
        , "Supervised_Fairness" boolean
        , primary key (model_id, subset_id, tie_breaker, evaluation_start_time, evaluation_end_time, parameter, attribute_name, attribute_value)
    );


end $train_results$;



raise notice 'populating schema test_results';
do $test_results$ begin
    set search_path = test_results, public;

    drop table if exists individual_importances;
    create table if not exists individual_importances (
        individual_importance_id serial
        , model_id integer not null references triage_metadata.models (model_id)
        , entity_id integer
        , as_of_date timestamp with time zone
        , feature  text
        , method   text
        , feature_value real
        , importance_score real
        , primary key (individual_importance_id, entity_id, as_of_date, feature, method)
    );

    drop table if exists predictions;
    create table if not exists predictionss (
        model_id             integer references triage_metadata.models (model_id)
        , entity_id          integer
        , as_of_date         timestamp with time zone
        , score              real
        , label_value        smallint
        , rank_abs_no_ties   smallint
        , rank_abs_with_ties smallint
        , rank_pct_no_ties   real
        , rank_pct_with_ties real
        , matrix_id          integer references triage_metadata.matrices (matrix_id)
        , test_label_timespan tstzrange
        , primary key (model_id, entity_id, as_of_date)
    );

    drop table if exists prediction_metadata;
    create table if not exists prediction_metadata (
        model_id              integer references triage_metadata.models (model_id)
        , matrix_id           integer references triage_metadata.matrices (matrix_id)
        , tiebreaker_ordering triage_metadata.tiebreaker_ordering
        , random_seed         integer
        , predictions_saved   boolean
        , primary key (model_id, matrix_id)
    );

    drop table if exists evaluations;
    create table if not exists evaluations (
        model_id                  integer references triage_metadata.models (model_id)
        , evaluation_start_time   timestamp with time zone
        , evaluation_end_time     timestamp with time zone
        , as_of_date_frequency    tstzrange
        , subset_id               integer references triage_metadata.subsets (subset_id)
        , metric                  triage_metadata.metric
        , parameter               text
        , matrix_id               integer references triage_metadata.matrices (matrix_id)
        , num_labeled_examples        integer
        , num_labeled_above_threshold integer
        , num_positive_labels         integer
        , sort_seed                   integer
        , best_value                  real
        , worst_value                 real
        , stochastic_value            real
        , num_sort_trials             smallint
        , standard_deviation          real
        , primary key(model_id, evaluation_start_time, evaluation_end_time, as_of_date_frequency, metric, parameter, subset_id)
    );

    drop table if exists aequitas;
    create table if not exists aequitas (
        model_id                  integer references triage_metadata.models (model_id)
        , subset_id               integer references triage_metadata.subsets (subset_id)
        , tie_breaker             triage_metadata.tiebreaker_ordering
        , evaluation_start_time   timestamp with time zone
        , evaluation_end_time     timestamp with time zone
        , as_of_date_frequency    tstzrange
        , parameter          text
        , attribute_name     text
        , attribute_value    text
        , matrix_id          integer not null references triage_metadata.matrices (matrix_id)
        , total_entities     integer
        , group_label_pos    integer
        , group_label_neg    integer
        , group_size         integer
        , group_size_pct     real
        , prev               integer
        , pp                 integer
        , pn                 integer
        , fp                 integer
        , fn                 integer
        , tn                 integer
        , tp                 integer
        , ppr                integer
        , pprev              integer
        , tpr                integer
        , tnr                integer
        , "for"              integer
        , fdr                integer
        , fpr                integer
        , fnr                integer
        , npv                integer
        , precision          real
        , ppr_disparity      real
        , ppr_ref_group_value text
        , pprev_disparity real
        , pprev_ref_group_value text
        , precision_disparity real
        , precision_ref_group_value text
        , fdr_disparity real
        , fdr_ref_group_value text
        , for_disparity real
        , for_ref_group_value text
        , fpr_disparity real
        , fpr_ref_group_value text
        , fnr_disparity real
        , fnr_ref_group_value text
        , tpr_disparity real
        , tpr_ref_group_value text
        , tnr_disparity real
        , tnr_ref_group_value text
        , npv_disparity real
        , npv_ref_group_value text
        , "Statistical_Parity" boolean
        , "Impact_Parity" boolean
        , "FDR_Parity" boolean
        , "FPR_Parity" boolean
        , "FOR_Parity" boolean
        , "FNR_Parity" boolean
        , "TypeI_Parity" boolean
        , "TypeII_Parity" boolean
        , "Equalized_Odds" boolean
        , "Unsupervised_Fairness" boolean
        , "Supervised_Fairness" boolean
        , primary key (model_id, subset_id, tie_breaker, evaluation_start_time, evaluation_end_time, parameter, attribute_name, attribute_value)
    );


end $test_results$;



exception when others then
    get stacked diagnostics exc_message = message_text;
    get stacked diagnostics exc_context = pg_exception_context;
    get stacked diagnostics exc_detail = pg_exception_detail;
    raise exception E'\n------\n%\n%\n------\n\nCONTEXT:\n%\n', exc_message, exc_detail, exc_context;
end $$;
