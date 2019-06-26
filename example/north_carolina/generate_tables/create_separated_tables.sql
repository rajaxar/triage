drop table if exists cleaned.sentences cascade;
drop table if exists cleaned.discipline cascade;
drop table if exists cleaned.offenses cascade;

create table cleaned.sentences(
	entity_id int primary key not null,
	INMATE_DOC_NUMBER varchar(7) not null,
	INMATE_COMMITMENT_PREFIX varchar(2) not null,
	SENTENCE_START date,
	SENTENCE_END date,
	INMATE_RACE_CODE varchar(10),
	INMATE_BIRTH_DATE date,
	INMATE_GENDER_CODE varchar(6),
	SENTENCE_TOTAL_COUNTS decimal,
	NEW_PERIOD_OF_INCARCERATION_FL varchar(3),
	P_P_COMMITMENT_STATUS_FLAG varchar(30),
	NEW_PERIOD_OF_SUPERVISION_FLAG varchar(3),
	MINIMUM_SENTENCE_LENGTH decimal,
	MAXIMUM_SENTENCE_LENGTH decimal,
	PAROLE_SUPERVISION_BEGIN_DATE date,
	PAROLE_DISCHARGE_DATE date,
	LENGTH_OF_SUPERVISION decimal);


create table cleaned.discipline(
	record_id int primary key not null,
	INMATE_DOC_NUMBER varchar(7) not null,
	INMATE_COMMITMENT_PREFIX varchar(2) not null,
	SENTENCE_START date,
	SENTENCE_END date,
	DISCIPLINARY_INFRACTION_DATE date,
	DISCIPLINARY_INFRACTION_CODE varchar(30),
	DISCI_SEGREGATION_TIME_DAYS decimal,
	INMATE_PLEA_RE_INFRACTION varchar(10),
	DISCIINFRACTION_VERDICT_CODE varchar(14));


create table cleaned.offenses(
	offense_id int not null primary key,
	entity_id int not null references cleaned.sentences(entity_id),
	INMATE_DOC_NUMBER varchar(7) not null,
	INMATE_COMMITMENT_PREFIX varchar(2),
	NUMBER_OF_COUNTS int,
	LENGTH_OF_SUPERVISION decimal,
	MINIMUM_SENTENCE_LENGTH int,
	MAXIMUM_SENTENCE_LENGTH decimal,
	PRIMARY_OFFENSE_CODE varchar(30),
	PRIMARY_FELONY_MISDEMEANOR_CD varchar(5),
	PRIOR_RCD_POINTS_CONVICTIONS int,
	COUNTY_OF_CONVICTION_CODE varchar(12),
	SERVING_MIN_OR_MAX_TERM_CODE varchar(9),
	SENTENCE_TYPE_CODE varchar(29),
	PUNISHMENT_TYPE_CODE varchar(18),
	COURT_TYPE_CODE varchar(10),
	OFFENSE_QUALIFIER_CODE varchar(10),
	SENTENCING_PENALTY_CLASS_CODE varchar(25),
	PRIOR_RECORD_LEVEL_CODE varchar(9));


  
CREATE TABLE cleaned.infractions AS
select s.entity_id, d.*
from cleaned.discipline as d
left join cleaned.sentences as s
    on s.inmate_doc_number = d.inmate_doc_number and s.inmate_commitment_prefix = d.inmate_commitment_prefix;
